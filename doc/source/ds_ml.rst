================
Machine Learning
================

:term:`Machine learning` is the study of constructing algorithms that can learn from data.

.. outdated::

    When you enter a search phrase or question in an internet search tool, you get a list of websites ranked in order of relevance.
    The search engine bases its list on the content of the site, the links in each sites' webpages, how often users visit these sites,
    how often users follow the suggested links in a query, and examples of queries and manually ranked webpages.
    More and more, :term:`machine learning` is being used to automate search engines.
    (See `Introduction to Machine Learning`_ [#ML1]_ )

When someone uses a search engine to perform a query, they are returned a
ranked list of websites, ordered according to predicted relevance.
Ranking these sites is typically done using page content, as well as the
relevance of other sites that link to a particular page.
Machine learning is used to automate this process, allowing search engine
companies to scale this process up to billions of potential web pages.

.. outdated::

    Internet bookstores or video rental sites use collaborative filtering to encourage users to buy more books, or rent more movies.
    The website needs to produce a list of suggestions, without having access to a specific query.
    So the site needs to use the customer's past purchasing behavior to predict future behavior.
    Additionally, the site might use the behavior of similar users to predict what this customer might find interesting.
    The business goal here is to automate this logic, so that the application behind the website can make these predictions without
    involving an analyst or other website personnel.

Online retailers often use a machine learning algorithm called collaborative
filtering to suggest products users might be interested in purchasing.
These suggestions are produced dynamically and without the use of a specific
input query, so retailers use a customer's purchase and browsing history,
along with those of customers with whom shared interests can be identified.
Implementations of collaborative filtering enable these recommendations to
be done automatically, without directly involving analysts.

.. outdated::

    There are many other problems that are amenable to :term:`machine learning` solutions.
    Translation of text for example is a difficult issue.
    It is possible to use examples of pre-translated to text to enable an algorithm to learn how to translate text from one language to another.
    This requires many good examples of translations, but ultimately, the software learns how to translate, at least for specific languages.

    To solve these and other problems, we need to be able to build software that can learn from data.

There are many other problems that are amenable to :term:`machine learning`
solutions.
Translation of text for example is a difficult issue.
A corpus of pre-translated text can be used to teach an algorithm a mapping
from one language to another.

---------------
Algorithm Types
---------------

.. toctree::
    :maxdepth: 2
    
    ds_mlal_0

.. outdated::

    The algorithms that we use in the Analytics Toolkit are :term:`Unsupervised Learning` or :term:`Supervised Learning` algorithms,
    where you either have definitive results (supervised) or where the results are determined by an estimation on the relationship
    of the data to be examined and not a specific known result (unsupervised).

    For more information `Wikipedia\: Machine Learning / Algorithm Types`_ has a longer discussion of algorithm types.

    :term:`Supervised Learning` - These algorithms are designed to teach the software to generalize from specific data.
    Then the generalized learning is applied to new problems.

    :term:`Unsupervised Learning` - Here the algorithm learns from data where the outcome is unknown.

    :term:`Semi-Supervised Learning` - In this case, some of the data given to the algorithm are known, as in supervised learning,
    and some are unknown, as in unsupervised learning.
    The idea here is that the software learns faster.

Intel Analytics Toolkit incorporates supervised, unsupervised, and
semi-supervised machine learning algorithms.
Supervised algorithms are used to learn the relationship between features in
a dataset and some labeling schema, such as is in classification.
For example, binary logistic regression builds a model for relating a linear
combination of input features (e.g., high and low temperatures for a
collection of days) to a known binary label (e.g., whether or not someone
went for a trail run on that day).
Once the relationship between temperature and running activity is learned,
then the model can be used to make predictions about new running activity,
given the days temperatures.
Unsupervised machine learning algorithms are used to find patterns or
groupings in data for which class labels are unknown.
For example, given a data set of observations about flowers (e.g., petal
length, petal width, sepal length, and sepal width), an unsupervised
clustering algorithm could be used to cluster observations according to
similarity.
Then, a researcher could look for reasonable patterns in the groupings, such
as "similar species appear to cluster together."
Semi-supervised learning is the natural combination of these two classes of
algorithms, in which unlabeled data are supplemented with smaller amounts of
labeled data, with the goal of increasing the accuracy of learning.
For more information on these approaches, the respective wikipedia entries
to these approaches provide an easy-to-read overview of their strengths and
limitations.

---------------
Other Resources
---------------

There is plenty of literature on :term:`machine learning` for those who want to gain a more thorough understanding of it.
We recommend: `Introduction to Machine Learning`_ and `Wikipedia\: Machine Learning`_.
You might find this link helpful as well: `Everything You Wanted to Know About Machine Learning, But Were Too Afraid To Ask (Part Two)`_.

.. _Introduction to Machine Learning: http://alex.smola.org/drafts/thebook.pdf
.. _Wikipedia\: Machine Learning: http://en.wikipedia.org/wiki/Machine_learning
.. _Everything You Wanted to Know About Machine Learning, But Were Too Afraid To Ask (Part Two): http://blog.bigml.com/2013/02/21/everything-you-wanted-to-know-about-machine-learning-but-were-too-afraid-to-ask-part-two/
.. _Wikipedia\: Machine Learning / Algorithm Types: http://en.wikipedia.org/wiki/Machine_learning#Algorithm_types

.. rubric:: Footnotes

.. [#ML1] Alex Smola and S.V.N. Vishwanathan (2008). thebook.pdf, Cambridge University Press, ISBN 0-521-82583-0.
