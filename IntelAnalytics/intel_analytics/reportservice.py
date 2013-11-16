class ReportService:
    def setReportStrategy(self, reportStrategy):
        self.reportStrategy = reportStrategy

    def reportLine(self, line):
        self.reportStrategy.report(line)

    def reportLines(self, lines):
        for line in lines:
            self.reportStrategy.report(line)