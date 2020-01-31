import spacy

# features to be extracted: location of accident, time of accident, number of casualties, severity of accident,
nlp = spacy.load("en_core_web_sm")


def parse_text(row, nlp):
    raw_text = row["raw_text"]
    doc = nlp(raw_text)

    return row