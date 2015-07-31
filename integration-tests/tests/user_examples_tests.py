import unittest
import trustedanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()



class UserExamples(unittest.TestCase):
    def test_frame(self):
        import trustedanalytics.examples.frame as frame_test
        frame_test.run("/datasets/cities.csv", ia)
        assert True

    def test_movie_graph_small(self):
        import trustedanalytics.examples.movie_graph_small as mgs
        vars = mgs.run("/datasets/movie_data_random.csv", ia)

        assert vars["frame"].row_count == 2
        assert vars["frame"].name == "MGS" and vars["graph"].name == "MGS"
        assert vars["graph"].vertex_count == 4
        assert vars["graph"].edge_count == 2


    def test_pr(self):
        import trustedanalytics.examples.pr as pr
        vars = pr.run("/datasets/movie_data_random.csv")

        assert vars["frame"].row_count == 20
        assert vars["frame"].name == "PR" and vars["graph"].name == "PR"
        assert vars["graph"].vertex_count == 29
        assert vars["graph"].edge_count == 20
        assert vars["result"]["vertex_dictionary"]["user_id"].row_count == 18
        assert vars["result"]["vertex_dictionary"]["movie_id"].row_count == 11
        assert vars["result"]["edge_dictionary"]["rating"].row_count == 20




