from argparse import ArgumentParser


class SparkArgParser:
    def __init__(self):
        self.parser = ArgumentParser()
        self.parser.add_argument('--ctl_loading', required=True)
        self.parser.add_argument('--ctl_loading_date', required=True)


    def parse_args(self, argv):
        return self.parser.parse_args(argv)