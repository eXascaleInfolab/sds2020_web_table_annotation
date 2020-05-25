from os.path import isdir, join, split
from gensim.models import Word2Vec

class Model(Word2Vec):

    @classmethod
    def load(cls, models_directory=None, filename=None):
        subdirectory = models_directory

        try:
            model = super(Model, cls).load(join(subdirectory, filename))
            model.metadata = {
                'filename': filename
            }
        except OSError:
            model = Model()
            model.metadata = {
                'error': 'File not found: {}'.format(filename)
            }
        return model
