from dataclasses import is_dataclass, fields
from typing import Generic, TypeVar, Type, Union

from pydantic import BaseModel, parse_obj_as

from da2.entity import Description
from taoAsync.taoAsync import Association, Object


def parseTaoModelAsDesc(
    model: Union[Object, Association], descClass: Type[Description]
) -> Description:
    dictionary: dict = model.data
    if is_dataclass(descClass):
        knownData = {}
        for field in fields(descClass):
            if field.name in dictionary \
                    and field.name not in ["createAt", "updateAt"]:
                knownData[field.name] = dictionary.get(field.name)
            else:
                try:
                    knownData[field.name] = getattr(model, field.name)
                except Exception:
                    continue
        result = descClass(**knownData)
        return result

    elif issubclass(descClass, BaseModel):
        result = parse_obj_as(descClass, dictionary)
        return result

    else:
        raise ValueError(f"Unsupported type: {descClass}")


def desc2dict(desc: Description) -> dict:

    if is_dataclass(desc):
        return {field.name: getattr(desc, field.name) for field in fields(desc)}

    elif type(desc) == BaseModel:
        return desc.dict()

    else:
        raise ValueError(f"Unsupported type: {desc}")
