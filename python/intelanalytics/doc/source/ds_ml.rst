Machine Learning
================

Machine learning is a branch of artificial intelligence.
It is about constructing and studying software that can "learn" from data.

When you enter a search phrase or question in an internet search tool, you get a list of websites ranked in order of relevance.
The search engine bases its list on the content of the site, the links in each sites' webpages, how often users visit these sites, "how often users follow the suggested links in a query, and examples of queries and manually ranked webpages."
More and more, machine learning is being used to automate search engines.
(See http://alex.smola.org/drafts/thebook.pdf.)

Internet bookstores or video rental sites use collaborative filtering to encourage users to buy more books, or rent more movies.
The website needs to produce a list of suggestions, without having access to a specific query.
So the site needs to use the customer's past purchasing behavior to predict future behavior.
Additionally, the site might use the behavior of similar users to predict what this customer might find interesting.
The business goal here is to automate this logic, so that the application behind the website can make these predictions without involving an analyst or other website personnel.

There are many other problems that are amenable to machine learning solutions.
Translation of text for example is a difficult issue.
It is possible to use examples of pre-translated to text to enable an algorithm to "learn" how to translate text from one language to another.
This requires many good examples of translations, but ultimately, the software learns how to translate, at least for specific languages.

To solve these and other problems, we need to be able to build software that can "learn" from data.
We also want to consider each problem by the type of data involved.
That way, when we encounter new problems, we can look at the type of data and previous solutions.
Even when we have similar problems, the data may use different measures, feet, inches, meters, pounds, kilograms, Euros, yen, dollars, or pesos.
To automate these problems and the solutions, we need to normalize the data.

Machine Learning as the Analytics Toolkit Uses It
-------------------------------------------------

There is plenty of literature on machine learning for those who want to gain a more thorough understanding of it.
We recommend: http://alex.smola.org/drafts/thebook.pdf and http://en.wikipedia.org/wiki/Machine_learning.
You might find this link helpful as well: http://blog.bigml.com/2013/02/21/everything-you-wanted-to-know-about-machine-learning-but-were-too-afraid-to-ask-part-two/.

Algorithm Types
---------------

The algorithms that we use in the Analytics Toolkit are Unsupervised Learning or Supervised Learning algorithms, where you either have definitive results (supervised) or where the results are determined by an estimation on the relationship of the data to be examined and not a specific known result (unsupervised).

For more information http://en.wikipedia.org/wiki/Machine_learning#Algorithm_types has a longer discussion of algorithm types. 

**Supervised Learning** - These algorithms are designed to teach the software to generalize from specific data.
Then the generalized learning is applied to new problems.

**Unsupervised Learning** - Here the algorithm learns from data where the outcome is unknown.
The idea here is to find new structure in the data.

**Semi-Supervised Learning** - In this case, some of the data given to the algorithm are known, as in supervised learning, and some are unknown, as in unsupervised learning.
The idea here is that the software learns faster.

.. todo:: Methods to implemen machine learning
    We implement machine learning in several classes and several methods.
    The following methods implement various algorithms for manipulating your data.

    The ``recommend()`` method is for making recommendations, such as movies, books, or guitars the user might find interesting. 

    The ``belief_prop()`` method performs belief propagation in a loop until the algorithm converges.
    You can use this for performing inference on graphical models, such as Bayesian networks and Markov random fields. 

    The ``page_rank()`` method is for ranking pages, as in a websearch.
    You can find details on this algorithm here:
    http://en.wikipedia.org/wiki/PageRank. 

    The ``avg_path_len()`` method calculates the average path length, that is, "the average number of steps along the shortest paths for all possible pairs of network nodes."
    See http://en.wikipedia.org/wiki/Average_path_length. 

    The ``label_prop()`` method performs label propagation on Gaussian random fields.
    Use this method to detect community structure in networks.
    For more details, see http://reports-archive.adm.cs.cmu.edu/anon/cald/abstracts/02-107.html.

    The lda() method performs latent Dirichlet allocation.
    For more information, see http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation. 

    The als() method performs the Alternating Least Squares with Bias for collaborative filtering algorithms.
    Use this for recommendation calculations.
    For more details, see: http://www.hpl.hp.com/personal/Robert_Schreiber/papers/2008%20AAIM%20Netflix/netflix_aaim08(submitted).pdf and http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf.

    The cgd() method performs conjugate gradient descent filtering.
    Use this for recommendation calculations. For more information see
    http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf.

.. todo:: Model Evaluation
    Model Evaluation
    ----------------

    Different models need different evaluation methods.
    What will be added in the first step will be prior and posterior histogram, RoC/AUC curves, and lift curves.
