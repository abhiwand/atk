"""
Class for giving report based on input string values,
which can be from for example stdout or stderr from executing
commands.
"""
class JobReportService:

    def __init__(self):
        self.report_strategy_list = []

    def add_report_strategy(self, report_strategy):
        """
        assign a strategy instance to be use
        :param report_strategy:
        """
        self.report_strategy_list.append(report_strategy)

    def report_line(self, line):
        """
        giving report with the assigned strategy for single input line
        :param line:
        """
        for strategy in self.report_strategy_list:
            if strategy:
                strategy.report(line)

    def report_lines(self, lines):
        """
        giving reports for multiple input lines
        :param lines:
        """
        for line in lines:
            self.report_strategy_list.report(line)

"""
Base report strategy class. It defines the signature
of reporting job status for input
"""
class ReportStrategy:
    def report(self, line):
        pass
    
class PrintReportStrategy:
    def report(self, line):
        print line    