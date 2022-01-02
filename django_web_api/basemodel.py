from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models.fields.related import ManyToManyField, OneToOneField, OneToOneRel
import uuid


class BaseModel(models.Model):
    _base_api_fields = (
        "uuid",
        "created_at",
        "updated_at",
        "pk",
    )
    exposed_fields = tuple()
    formatters = dict()
    api_annotations = dict()

    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

    @classmethod
    def is_exposed(cls, field_str):
        full_path = field_str.split("__")

        for index, field in enumerate(full_path):
            if not field in cls._all_fields:
                return False

            if index + 1 == len(full_path):
                break

            field_type = cls._meta.get_field(field)
            if field_type.is_relation:
                return field_type.related_model.is_exposed("__".join(full_path[index + 1:]))
        return True

    @classmethod
    def _compute_fields(cls):
        direct_fields = set(["pk"])
        relateds_fields = set()
        backwards_fields = set()
        through_ordering = dict()
        foreign_key_fields_name = set()
        many_to_many_fields = set()
        needs_serialization = set()
        property_fields = set()
        subclasses_fields = set()

        try:
            all_fields = cls._base_api_fields + cls.exposed_fields
        except Exception as e:
            print(f"Error while computing fields for {cls}")
            print(e)

        # Find subclasses for inheritances
        for field in cls._meta.get_fields():
            if not isinstance(field, OneToOneRel):
                continue
            rel = field

            if isinstance(rel.field, OneToOneField) \
                and issubclass(rel.field.model, cls) \
                and cls is not rel.field.model \
                and rel.parent_link:
                subclasses_fields.add(rel.field)

        # Compute API Fields
        for field_name in all_fields:
            if field_name == "pk":
                continue

            try:
                field = cls._meta.get_field(field_name)

                if type(getattr(cls, field_name, None)) == property:
                    raise Exception(f"Property '{field_name}' clashes with a django model field in class {cls}.")

                if isinstance(field, models.BinaryField):
                    needs_serialization.add(field_name)

                if field.is_relation:
                    relateds_fields.add(field)

                    if isinstance(field, ManyToManyField) and issubclass(getattr(cls, field_name).through, BaseModel):
                        backward_through_name = field.remote_field.get_path_info()[1].join_field.related_query_name()
                        ordering = field.remote_field.through._meta.ordering
                        if ordering:
                            through_ordering[field_name] = [f"{backward_through_name}__{ordering_name}" for ordering_name in ordering]

                    multiple = getattr(field, "multiple", False)
                    if field.many_to_many or multiple:
                        many_to_many_fields.add(field)
                        if multiple:
                            backwards_fields.add(field_name)
                        continue
                    else:
                        foreign_key_fields_name.add(field_name)
                direct_fields.add(field_name)
            except FieldDoesNotExist:
                if hasattr(cls, field_name):
                    property_fields.add(field_name)
                else:
                    print("Unknown field", field_name, "in", cls)

        cls._direct_fields       = direct_fields
        cls._relateds_fields     = relateds_fields
        cls._property_fields     = property_fields
        cls._through_ordering    = through_ordering

        if getattr(cls, "writable_fields", False):
            cls._writable_fields = set(cls.writable_fields) - property_fields
        else:
            cls._writable_fields = set(cls.exposed_fields) - property_fields

        cls._m2m_fields          = many_to_many_fields
        cls._fk_fields_name      = foreign_key_fields_name
        cls._needs_serialization = needs_serialization
        cls._backwards_field     = backwards_fields
        cls._all_fields          = all_fields
        cls._subclasses_fields   = subclasses_fields

