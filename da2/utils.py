
from da2.entity import Identity
from taoAsync.objectType import ObjectType
from taoAsync.objectTypeSequence import ObjectTypeSequence
from utils.convertStrIdToInt import convertId


def verifyOtype(identity: Identity, otype: ObjectType):
    convertedId = convertId(identity)
    identitySequence = convertedId & 0b11111111111111
    otypeSequence = getattr(ObjectTypeSequence, otype.value).value
    return identitySequence == otypeSequence


def identifyOtype(identity: Identity):
    convertedId = convertId(identity)
    identitySequence = convertedId & 0b11111111111111
    otype = ObjectTypeSequence(identitySequence)
    return otype.name