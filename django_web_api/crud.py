from django.apps import apps
from django.db import IntegrityError
from .exceptions import ApiException
from .basehandler import BaseHandler
from .serializers import serialize, serialize_relateds, sanitize_qs
from django.db.models import QuerySet, BinaryField
import importlib
import base64

ALLOWED_ACTIONS = ("create", "read", "update", "delete", "filter", "preview")
VALID_OPERATORS = ("in", "eq", "lt", "lte", "gt", "gte", "isnull", "contains", "icontains",)

def format_creation_args(model, dictionary):
    args = dict()
    for key in list(dictionary):
        if not key in model._writable_fields and not key in ("pk", "uuid"):
            continue

        val = dictionary[key]
        field = model._meta.get_field(key)

        if isinstance(field, BinaryField):
            args[key] = base64.b64decode(val)
            continue

        args[key] = val

    return args

class Handler(BaseHandler):
    prevent_serialization = True

    def execute(self, action, model, data):
        try:
            app, model_name = model.split(".")
        except ValueError as e:
            raise Exception(f"Wrong model name : {model}")
        model = apps.get_model(app_label=app, model_name=model_name)
        self.model = model

        if not model.exposed_fields:
            raise Exception(f"The model {model_name} doesn't expose any fields")

        if action in ("create", "update",):
            if hasattr(model, "_crud__pre_save"):
                getattr(model, "_crud__pre_save")(**data)

        if hasattr(model, f"_crud__{action}"):
            return serialize(getattr(model, f"_crud__{action}")(self.request, **data), self.request.user)

        crud_method_return = getattr(self, action)(model, **data)

        if action in ("create", "update",):
            if hasattr(model, "_crud__post_save"):
                post_data = getattr(model, "_crud__post_save")(self.request, crud_method_return, **data)

                if post_data is not None:
                    crud_method_return = post_data


            crud_method_return = serialize({
                "data": crud_method_return
            }, self.user)

        return crud_method_return

    def check_permissions(self, args):
        if not self.request.user.is_authenticated:
            raise ApiException("User not authenticated", 401)

        action = args["action"]

        if not action in ALLOWED_ACTIONS:
            raise Exception(f"{action} is not an allowed action.")

        if action in ("filter", "preview"):
            action = "read" # Same permissions for filter & read

        app, model = args["model"].split(".")

        return f"crud:{app}__{model}__{action}" in self.request.session.get("permissions", [])

    def get_or_create_model(self, model, obj_list):
        models = list()
        to_create = list()
        for obj in obj_list:
            if type(obj) is str:
                models.append(model.objects.get(pk=obj))
            elif type(obj) is dict:
                obj = format_creation_args(model, obj)
                if "uuid" in obj:
                    inst = self.update(model, obj)
                else:
                    inst = model(**obj)
                    to_create.append(inst)
                models.append(inst)

        return models, to_create

    def format_response(self, data, with_relateds=False, fields_filter=None):
        rel_dict = None

        if with_relateds:
            rel_dict = dict()

        data = serialize(data, self.user, rel_dict, fields_filter)

        resp = {
            "data": data,
        }

        if with_relateds:
            resp["relateds"] = serialize_relateds(rel_dict)

        return resp

    def create(self, model, fields):
        creation_args = dict()
        m2m_set = dict()
        post_create = list()
        for field_name, value in fields.items():
            if not field_name in model._writable_fields:
                continue

            if field_name in model.formatters:
                value = model.formatters[field_name](value)

            field = model._meta.get_field(field_name)
            if field.related_model != None:
                foreign_model = field.related_model

            if field.many_to_one or field.one_to_one:
                value = foreign_model.objects.get(pk=value)
            elif field.one_to_many: # Reverse
                post_create.append((field, value))
                continue
            elif field.many_to_many:
                value, bulk_create = self.get_or_create_model(foreign_model, value)
                m2m_set[field_name] = value
                if bulk_create:
                    foreign_model.objects.bulk_create(bulk_create)
                continue

            creation_args[field_name] = value

        creation_args = format_creation_args(model, creation_args)
        instance = model(**creation_args)
        instance.full_clean()
        instance.save()

        for field, objs in post_create:
            foreign_name = field.field.name
            foreign_instances, bulk_create = self.get_or_create_model(field.related_model, objs)

            for foreign_instance in foreign_instances:
                setattr(foreign_instance, foreign_name, instance)

            if bulk_create:
                field.related_model.objects.bulk_create(bulk_create)

        for field_name, instances in m2m_set.items():
            getattr(instance, field_name).set(instances)

        instance.save()
        return instance

    def read_queryset(self, model, filters, limit=-1, start=0, get=False):
        filters_dict = {}
        exclude_dict = {}
        for f in filters:
            assert "field" in f, "Missing property 'field' in CRUD filter"
            assert "operator" in f, "Missing property 'operator' in CRUD filter"
            assert "value" in f, "Missing property 'value' in CRUD filter"

            if f['operator'] not in VALID_OPERATORS:
                raise ApiException(f"{f['operator']} is not a supported operator.", 400)
            filter_name = f['field']

            if not model.is_exposed(filter_name):
               raise ApiException(f"Field {filter_name} is not valid for {model.__name__}", 400)

            if f['operator'] != "eq":
                filter_name += "__{}".format(f['operator'])

            if f.get('exclude', False):
                exclude_dict[filter_name] = f['value']
            else:
                filters_dict[filter_name] = f['value']

        qs = model.objects.filter(**filters_dict).exclude(**exclude_dict)
        qs = sanitize_qs(qs, self.request.user)

        if get:
            return qs.get()

        if limit > 0:
            qs = qs[start:start+limit]
        else:
            qs = qs[start:]

        return qs

    def read(self, model, filters, limit=-1, start=0, relateds=False):
        return self.format_response(
            self.read_queryset(model, filters, limit, start, get=True),
            relateds,
        )

    def filter(self, model, filters, limit=-1, start=0, relateds=False):
        return self.format_response(
            self.read_queryset(model, filters, limit, start),
            relateds
        )

    def preview(self, model, filters, fields, limit=-1, start=0, relateds=False):
        return self.format_response(
            self.read_queryset(model, filters, limit, start),
            relateds,
            fields_filter=fields
        )

    def update(self, model, fields):
        update_args = dict()
        m2m_set = dict()
        post_update = list()

        fields = format_creation_args(model, fields)
        instance = model.objects.get(pk=fields['uuid'])
        for field_name, value in fields.items():
            if not field_name in model._writable_fields:
                continue

            if field_name in model.formatters:
                value = model.formatters[field_name](value)

            field = model._meta.get_field(field_name)

            if field.related_model != None:
                foreign_model = field.related_model

            if field.many_to_one or field.one_to_one:
                value = foreign_model.objects.get(pk=value)
            elif field.one_to_many: # Reverse
                post_update.append((field, value))
                continue
            elif field.many_to_many:
                value, bulk_create = self.get_or_create_model(foreign_model, value)
                m2m_set[field_name] = value
                if bulk_create:
                    foreign_model.objects.bulk_create(bulk_create)
                continue

            setattr(instance, field_name, value)

        # Update reverse relationships
        for field, objs in post_update:
            foreign_name = field.remote_field.name
            foreign_instances, bulk_create = self.get_or_create_model(field.related_model, objs)

            previous_instances = getattr(instance, field.name).all()

            for prev_instance in previous_instances:
                if prev_instance in foreign_instances:
                    continue
                setattr(prev_instance, foreign_name, None) # Break relations that does not exists anymore

            for foreign_instance in foreign_instances:
                setattr(foreign_instance, foreign_name, instance) # Create new relations

            if bulk_create:
                field.related_model.objects.bulk_create(bulk_create)
            field.related_model.objects.bulk_update(foreign_instances, [foreign_name])
            field.related_model.objects.bulk_update(previous_instances, [foreign_name])

        for field_name, instances in m2m_set.items():
            getattr(instance, field_name).set(instances)

        instance.full_clean()
        instance.save()
        return instance

    def delete(self, model, filters, limit=-1, start=0):
        queryset = self.read_queryset(model, filters, limit, start)
        deleteds = queryset.delete()
        return {
            "length": deleteds[0]
        }

