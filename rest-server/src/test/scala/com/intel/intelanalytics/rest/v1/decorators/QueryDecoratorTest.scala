/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.rest.v1.decorators

import com.intel.intelanalytics.domain.command.Command
import com.intel.intelanalytics.domain.query.Query
import com.intel.intelanalytics.rest.v1.viewmodels.RelLink
import org.joda.time.DateTime
import org.scalatest.{ FlatSpec, Matchers }

class QueryDecoratorTest extends FlatSpec with Matchers {

  val uri = "http://www.example.com/queries"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val query = new Query(1, "name", None, None, true, Some(2), None, new DateTime, new DateTime)
  val query2 = new Query(2, "name2", None, None, true, Some(3), None, new DateTime, new DateTime)

  "QueryDecorator" should "be able to decorate a query" in {
    val decoratedQuery = QueryDecorator.decorateEntity(null, relLinks, query)
    decoratedQuery.id should be(Some(1))
    decoratedQuery.name should be("name")
    decoratedQuery.links.head.uri should be("http://www.example.com/queries")
  }

  it should "return a list when decorating all queries" in {
    val decoratedQueries = QueryDecorator.decorateForIndex(uri, List(query, query2))
    decoratedQueries.length should be(2)
    decoratedQueries(0).id should be(1)
    decoratedQueries(0).name should be("name")
    decoratedQueries(0).url should be(uri + "/1")
  }

  it should "return a list of page urls when requesting pages" in {
    val decoratedPages = QueryDecorator.decoratePages(uri + "/1/data", query)
    decoratedPages.length should be(2)
    decoratedPages(0).id should be(1)
    decoratedPages(0).url should be(uri + "/1/data/1")
  }

  it should "return data when requesting a single page" in {
    val decoratedPage = QueryDecorator.decoratePage(uri, relLinks, query, 1, List(), None)
    decoratedPage.links.head.uri should be(uri)
    decoratedPage.result should not be (None)
    decoratedPage.result.get.page should be(Some(1))
    decoratedPage.result.get.totalPages should be(Some(2))
    decoratedPage.result.get.data should be(Some(List()))
  }

}
