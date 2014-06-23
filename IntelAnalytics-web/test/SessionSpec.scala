import scala.collection.immutable.HashSet

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/6/13
 * Time: 4:56 PM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class SessionSpec extends Specification with Mockito {

  "Search session by id" should {

    "create" in {

      running(FakeApplication()) {
        DB.withConnection {
          implicit c =>

            val num = Sessions.create(123)

            //create a session row
            val sessionObj = Sessions.read(num.get)
            (sessionObj != None) must beEqualTo(true)
            sessionObj.get.uid must beEqualTo(123)
        }
      }
    }

    "delete" in {

      running(FakeApplication()) {
        DB.withConnection {
          implicit c =>

            val num = Sessions.create(123)
            Sessions.delete(num.get)
            val deleted = Sessions.read(num.get)
            (deleted == None) must beEqualTo(true)
        }
      }
    }

    "update" in {

      running(FakeApplication()) {
        DB.withConnection {
          implicit c =>

            val num = Sessions.create(123)
            val sessionRow = SessionRow(num.get, 123, "test content", 11111111)
            Sessions.update(sessionRow)
            val sessionObj = Sessions.read(num.get)
            (sessionObj != None) must beEqualTo(true)
            sessionObj.get.uid must beEqualTo(123)
            sessionObj.get.Id must beEqualTo(num.get)
            sessionObj.get.data must beEqualTo("test content")
        }
      }
    }

    "create random session id" in {
      running(FakeApplication()) {
        val valuesSet = new HashSet[String]
        var foundDuplicate: Boolean = false

        for (a <- 1 to 100000) {
          val id = Sessions.createSessionId()
          if (valuesSet.contains(id)) {
            foundDuplicate = true
            //todo: find a way to break the loop
          }

          valuesSet + (id)
        }

        foundDuplicate must beFalse
      }
    }
  }
}

