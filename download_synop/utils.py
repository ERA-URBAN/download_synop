def fitem(item):
    try:
        item = item.strip()
    except AttributeError:
        pass
    try:
        item = float(item)
    except ValueError:
        pass
    return item


def is_number(s):
    '''
    check if the value in the string is a number and return True or False
    '''
    try:
        float(s)
        return True
    except ValueError:
        pass
    return False
