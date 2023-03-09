def has_field_attr(cls: object, field: str, attr: str):
    """
    Checks the given field attribute on a class.  If the attribute does not
    exist on the class, check the extended classes and return the first value
    for an existing attribute.
    """
    if hasattr(cls, field) and hasattr(getattr(cls, field), attr):
        return True
    for base in cls.__bases__:
        if hasattr(base, field) and hasattr(getattr(base, field), attr):
            return True
    return False

def get_field_attr(cls: object, field: str, attr: str):
    """
    Gets the given field attribute on a class.  If the attribute does not exist
    on the class, check the extended classes and return the first value for an
    existing attribute.
    """
    if hasattr(cls, field) and hasattr(getattr(cls, field), attr):
        return getattr(getattr(cls, field), attr)
    for base in cls.__bases__:
        if hasattr(base, field) and hasattr(getattr(base, field), attr):
            return getattr(getattr(base, field), attr)
    raise ValueError("Does not exist: attr: " + attr + " on field : " + str(field) + " for cls : " + str(cls))
