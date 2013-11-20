"""
Class for giving report based on input string values,
which can be from for example stdout or stderr from executing
commands.
"""
class ReportService:
    def setReportStrategy(self, reportStrategy):
        """
        assign a strategy instance to be use
        :param reportStrategy:
        """
        self.reportStrategy = reportStrategy

    def reportLine(self, line):
        """
        giving report with the assigned strategy for single input line
        :param line:
        """
        self.reportStrategy.report(line)

    def reportLines(self, lines):
        """
        giving reports for multiple input lines
        :param lines:
        """
        for line in lines:
            self.reportStrategy.report(line)