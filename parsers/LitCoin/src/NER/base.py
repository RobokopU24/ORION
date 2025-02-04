# The Base NER engine.

class BaseNEREngine:
    def annotate(self, text, props, limit):
        """
        Annotate the provided text.

        :param text: The text to annotate. Depending on the engine, this might be a word,
            phrase, paragraph or article.
        :param props: The list of properties to set. TBD.
        :return: A list in the form [{
            'text': "The text being annotated.",
            'id': "The CURIE for the annotated object",
            'label': "The label for the CURIE",
            'score': "The score of the match",
            'span': {
                'begin': "Index in the submitted text where this text begins.",
                'end': "Index in the submitted text where this text ends."
            },
            'summary': "A summary of the match.",
        }]
        """
        pass

