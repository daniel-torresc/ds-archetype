def convert_to_list(param: [str, list]) -> list:
    if not param:
        return []

    if isinstance(param, str):
        return list(map(str.strip, param.split(",")))
    elif isinstance(param, list):
        return param
    else:
        raise TypeError("Input value type must be one of [str, list]")
