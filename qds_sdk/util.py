import re
import optparse
import sys

class OptionParsingError(RuntimeError):
    def __init__(self, msg):
        self.msg = msg


class OptionParsingExit(Exception):
    def __init__(self, status, msg):
        self.msg = msg
        self.status = status


class GentleOptionParser(optparse.OptionParser):
    def error(self, msg):
        raise OptionParsingError(msg)

    def exit(self, status=0, msg=None):
        raise OptionParsingExit(status, msg)


# Patterns blatently stolen from Rails' Inflector
PLURALIZE_PATTERNS = [
    (r'(quiz)$', r'\1zes'),
    (r'^(ox)$', r'\1en'),
    (r'([m|l])ouse$', r'\1ice'),
    (r'(matr|vert|ind)(?:ix|ex)$', r'\1ices'),
    (r'(x|ch|ss|sh)$', r'\1es'),
    (r'([^aeiouy]|qu)y$', r'\1ies'),
    (r'(hive)$', r'1s'),
    (r'(?:([^f])fe|([lr])f)$', r'\1\2ves'),
    (r'sis$', r'ses'),
    (r'([ti])um$', r'\1a'),
    (r'(buffal|tomat)o$', r'\1oes'),
    (r'(bu)s$', r'\1ses'),
    (r'(alias|status)$', r'\1es'),
    (r'(octop|vir)us$', r'\1i'),
    (r'(ax|test)is$', r'\1es'),
    (r's$', 's'),
    (r'$', 's')
]

SINGULARIZE_PATTERNS = [
    (r'(quiz)zes$', r'\1'),
    (r'(matr)ices$', r'\1ix'),
    (r'(vert|ind)ices$', r'\1ex'),
    (r'^(ox)en', r'\1'),
    (r'(alias|status)es$', r'\1'),
    (r'(octop|vir)i$', r'\1us'),
    (r'(cris|ax|test)es$', r'\1is'),
    (r'(shoe)s$', r'\1'),
    (r'(o)es$', r'\1'),
    (r'(bus)es$', r'\1'),
    (r'([m|l])ice$', r'\1ouse'),
    (r'(x|ch|ss|sh)es$', r'\1'),
    (r'(m)ovies$', r'\1ovie'),
    (r'(s)eries$', r'\1eries'),
    (r'([^aeiouy]|qu)ies$', r'\1y'),
    (r'([lr])ves$', r'\1f'),
    (r'(tive)s$', r'\1'),
    (r'(hive)s$', r'\1'),
    (r'([^f])ves$', r'\1fe'),
    (r'(^analy)ses$', r'\1sis'),
    (r'((a)naly|(b)a|(d)iagno|(p)arenthe|(p)rogno|(s)ynop|(t)he)ses$',
     r'\1\2sis'),
    (r'([ti])a$', r'\1um'),
    (r'(n)ews$', r'\1ews'),
    (r's$', r'')
]

IRREGULAR = [
    ('person', 'people'),
    ('man', 'men'),
    ('child', 'children'),
    ('sex', 'sexes'),
    ('move', 'moves'),
]

UNCOUNTABLES = ['equipment', 'information', 'rice', 'money', 'species',
                'series', 'fish', 'sheep']


def pluralize(singular):
    """Convert singular word to its plural form.

    Args:
        singular: A word in its singular form.

    Returns:
        The word in its plural form.
    """
    if singular in UNCOUNTABLES:
        return singular
    for i in IRREGULAR:
        if i[0] == singular:
            return i[1]
    for i in PLURALIZE_PATTERNS:
        if re.search(i[0], singular):
            return re.sub(i[0], i[1], singular)


def singularize(plural):
    """Convert plural word to its singular form.

    Args:
        plural: A word in its plural form.
    Returns:
        The word in its singular form.
    """
    if plural in UNCOUNTABLES:
        return plural
    for i in IRREGULAR:
        if i[1] == plural:
            return i[0]
    for i in SINGULARIZE_PATTERNS:
        if re.search(i[0], plural):
            return re.sub(i[0], i[1], plural)
    return plural


def camelize(word):
    """Convert a word from lower_with_underscores to CamelCase.

    Args:
        word: The string to convert.
    Returns:
        The modified string.
    """
    return ''.join(w[0].upper() + w[1:]
                   for w in re.sub('[^A-Z^a-z^0-9^:]+', ' ', word).split(' '))


def underscore(word):
    """Convert a word from CamelCase to lower_with_underscores.

    Args:
        word: The string to convert.
    Returns:
        The modified string.
    """
    return re.sub(r'\B((?<=[a-z])[A-Z]|[A-Z](?=[a-z]))',
                  r'_\1', word).lower()

def _make_minimal(dictionary):
    """
    This function removes all the keys whose value is either None or an empty
    dictionary.
    """
    new_dict = {}
    for key, value in dictionary.items():
        if value is not None:
            if isinstance(value, dict):
                new_value = _make_minimal(value)
                if new_value:
                    new_dict[key] = new_value
            else:
                new_dict[key] = value
    return new_dict

def _read_file(file_path):
    file_content = None
    if file_path is not None:
        try:
            with open(file_path) as f:
                file_content = f.read()
        except IOError as e:
            sys.stderr.write("Unable to read %s: %s\n" % (file_path, str(e)))
            raise IOError("Unable to read %s: %s\n" % (file_path, str(e)))
    return file_content

