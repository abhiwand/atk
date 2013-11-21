"""
Class for giving report based on input string values,
which can be from for example stdout or stderr from executing
commands.
"""
class ReportService:
    def set_report_strategy(self, report_strategy):
        """
        assign a strategy instance to be use
        :param report_strategy:
        """
        self.report_strategy = report_strategy

    def report_line(self, line):
        """
        giving report with the assigned strategy for single input line
        :param line:
        """
        self.report_strategy.report(line)

    def report_lines(self, lines):
        """
        giving reports for multiple input lines
        :param lines:
        """
        for line in lines:
            self.report_strategy.report(line)