import gensim.parsing.preprocessing as gsp
from gensim import utils

filters = [
    gsp.strip_tags,
    gsp.strip_punctuation,
    gsp.strip_multiple_whitespaces,
    gsp.strip_numeric,
    gsp.remove_stopwords,
    gsp.strip_short,
    gsp.stem_text
]

vehicle_list = ["ambulance", "armored car", "autoautomobile", "bicycle", "bike", "bus", "bulldozer", "cab", "camper", "car", "carrier", "compact", "convertible", "cycle", "truck", "fire engine", "van", "forklift", "freighter", "garbage truck", "gondola", "handcar", "wagon", "jeep", "minibus", "minivan", "moped", "motorcar", "motorcycle", "tanker", "pickup truck", "racecar", "roadster", "RV", "school bus", "scooter", "sedan", "semi", "limo", "SUV", "taxi", "tram", "tricycle", "vehicle", "unicycle", "vespa"]

def clean_text(x, train=False):
    if train:
        s = x[1]
    else:
        s = x
    s = s.lower().replace('#', '')
    s = utils.to_unicode(s)
    s_ = s
    for f in filters:
        s_ = f(s_)
    if train:
        return x[2], s_, s
    return s_, s


def construct_parse_tree(df, nlp):
    information = df.select("raw_text").head()[0]
    doc = nlp(information)
    loc = ""
    vehicles = ""
    casualties = 0
    severity = ""
    for token in doc:
        if token.text in vehicle_list:
            vehicles += token.text + ", "
    return loc, vehicles, casualties, severity