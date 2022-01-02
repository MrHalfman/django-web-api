from collections import OrderedDict
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db import NotSupportedError
from django.db.models import Q, Model
from django.db.models.query import ValuesIterable, QuerySet

def serialize_polymorphic_qs(qs, rel_dict=None, filtered_fields=None):
    objs = list()
    pks = qs.values("pk")

    for field in qs.model._subclasses_fields:
        objs += serialize_qs(field.model.objects.filter(pk__in=pks), rel_dict, filtered_fields)

    return objs

def serialize_qs(qs, rel_dict = None, filtered_fields = None):
    if qs._iterable_class is ValuesIterable:
        return list(qs)

    model = qs.model

    if model._subclasses_fields:  # Serialize each type of subclasses independently
        return serialize_polymorphic_qs(qs, rel_dict, filtered_fields)

    model_name = str(model._meta)
    annotations = model.api_annotations.copy()

    if not filtered_fields:
        filtered_fields = model._all_fields

    fields = model._direct_fields & set(filtered_fields)

    # Fetch all pks for m2m
    pythonic_distinct_fields = list()
    for field in model._m2m_fields:
        field_name = field.name
        if not field_name in filtered_fields:
            continue

        filter_args = dict()
        filter_args[field_name] = None
        rel_model = field.related_model

        f_name = f"{field_name}_pks"
        if field_name in model._through_ordering:
            ordering = model._through_ordering[field_name]
        elif rel_model._meta.ordering:
            ordering = list()
            for ordering_rule in rel_model._meta.ordering:
                rel_name = field_name
                if ordering_rule[0] == "-":
                    ordering_rule = ordering_rule[1:]
                    rel_name = "-" + field_name
                ordering.append(f"{rel_name}__{ordering_rule}")
        else:
            ordering = list()

        if ordering:
            annotations[f_name] = ArrayAgg(field_name, filter=~Q(**filter_args), distinct=False, ordering=ordering)
            pythonic_distinct_fields.append(f_name) # Cannot do distinct=True + ordering with ArrayAgg, distinct is made below in Python
        else:
            annotations[f_name] = ArrayAgg(field_name, filter=~Q(**filter_args), distinct=True)
    try:
        qs = qs.annotate(**annotations)
    except NotSupportedError:
        # Sometimes annotate is not supported on specific QS ( .difference for example)
        # Making a new request to get a clean QS is still faster
        return serialize_qs(model.objects.filter(pk__in=qs.values("pk")), rel_dict, filtered_fields)

    fields.update(qs.query.annotations.keys())

    if not qs.query.is_sliced and not qs.ordered:
        # Slicing already applies default ordering
        qs = qs.order_by(*model._meta.ordering)

    vals = list(qs.values(*fields))

    for obj in vals:
        obj["_model_name"] = model_name

        for field_name in pythonic_distinct_fields:
            obj[field_name] = list(OrderedDict.fromkeys(obj[field_name])) # Make PKs unique

        for field_name in model._property_fields:
            if not field_name in filtered_fields:
                continue
            obj[field_name] = serialize(getattr(model, field_name)(obj))

        for field_name in model._needs_serialization:
            if not field_name in filtered_fields:
                continue

            obj[field_name] = serialize(obj[field_name])

        if type(rel_dict) is not dict:
            continue

        for field in model._relateds_fields:
            field_name = field.name
            if not field_name in filtered_fields:
                continue

            rel_model = field.related_model
            if not rel_model in rel_dict:
                rel_dict[rel_model] = set()
            if field.many_to_many or getattr(field, "multiple", False):
                pks = obj[field_name + "_pks"]
            else:
                pks = [obj[field_name]]
            rel_dict[rel_model] = rel_dict[rel_model].union(set(pks))

    return vals


def sanitize_qs(qs, user=None):
    if not user:
        return qs

    if hasattr(qs.model, "_api_sanitize"):
        qs = qs.model._api_sanitize(qs, user)

    return qs

def serialize(obj, user=None, relateds_dict=None, qs_fields_filter=[], sanitize=True):
    if type(obj) in (int, float, str, None, bool):
        return obj

    if isinstance(obj, BaseModel):
        model = obj._meta.model
        qs = model.objects.filter(pk=obj.pk)
        if sanitize:
            qs = sanitize_qs(qs, user)
        try:
            inst = serialize_qs(qs, relateds_dict, qs_fields_filter)[0]
        except IndexError:
            inst = None
            # raise ApiException(f"This {model.__name__} does not exists.", 404)
        return inst
    if type(obj) is memoryview:
        return base64.encodebytes(obj).decode("utf-8")

    if type(obj) is dict:
        for key, value in obj.items():
            obj[key] = serialize(value, user, relateds_dict, qs_fields_filter, sanitize)
        return obj

    if type(obj) in (QuerySet):
        if sanitize:
            obj = sanitize_qs(obj, user)
        return serialize_qs(obj, relateds_dict, qs_fields_filter)

    if type(obj) in (set, list, tuple):
        return [serialize(el, user, relateds_dict, qs_fields_filter, sanitize) for el in obj]

    return obj

def serialize_relateds(rel_dict):
    items = list()
    for model, pks in rel_dict.items():
        items += serialize_qs(model.objects.filter(pk__in=pks))
    return items
