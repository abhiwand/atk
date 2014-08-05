===========================
Machine Learning Algorithms
===========================


In this release of the Analytics Toolkit, we support eight graphical algorithms in iGiraph.
From a functionality point of view, they fall into these categories: *Collaborative Filtering*, *Graph Analytics*, *Graphical Models*, and *Topic Modeling*.

* :ref:`Collaborative_Filtering`

    * :ref:`ALS`
    * :ref:`CGD`

* :ref:`Graph_Analytics`

    * :ref:`PR`

.. TODO::
    * ref:`APL`
    * ref:`CC`


* :ref:`Graphical_Models`

    * :ref:`LP`
    * :ref:`LBP`

* :ref:`Topic_Modeling`
    * :ref:`LDA`


.. _Collaborative_Filtering:

-----------------------
Collaborative Filtering
-----------------------

Collaborative Filtering is widely used in recommender systems.
For more information see: `Wikipedia\: Collaborative Filtering`_.

We support two methods in this category, :ref:`ALS` and :ref:`CGD`

.. _ALS:
.. include:: ds_mlal_als.rst


.. _CGD:
.. include:: ds_mlal_cgd.rst


.. _Graph_Analytics:

---------------
Graph Analytics
---------------

    We currently support one algorithm in this category, :ref:`PR`


.. _PR:
.. include:: ds_mlal_pr.rst

.. TODO::

    We support three algorithms in this category, ref:`APL`, ref:`CC`, and :ref:`PR`

    .. _APL:
    .. include:: ds_mlal_apl.rst


    .. _CC:
    .. include:: ds_mlal_cc.rst


.. _Graphical_Models:

----------------
Graphical Models
----------------


The graphical models find more insights from structured noisy data.
We currently support :ref:`LP` and :ref:`LBP`

.. _LP:
.. include:: ds_mlal_lp.rst


.. _LBP:
.. include:: ds_mlal_lbp.rst


.. _GLBP:
.. include:: ds_mlal_glbp.rst


.. _Topic_Modeling:

--------------
Topic Modeling
--------------


For Topic Modeling, see: http://en.wikipedia.org/wiki/Topic_model

.. _LDA:
.. include:: ds_mlal_lda.rst




.. _Wikipedia\: Collaborative Filtering: http://en.wikipedia.org/wiki/Collaborative_filtering
.. _Columbia Data Science\: Blog Week-7: http://columbiadatascience.com/2012/10/18/week-7-hunch-com-recommendation-engines-svd-alternating-least-squares-convexity-filter-bubbles/
.. _Factorization Meets the Neighborhood\: a Multifaceted Collaborative Filtering Model: http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf
.. _Large-Scale Parallel Collaborative Filtering for the Netflix Prize: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797

.. rubric:: footnotes

.. [#] http://www.cs.cmu.edu/~zhuxj/pub/CMU-CALD-02-107.pdf
.. [#] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf
.. [#] http://www.sciencedirect.com/science/article/pii/S1532046401910299
.. [#] http://tartarus.org/~martin/PorterStemmer/index.html
.. [#] http://www.textfixer.com/resources/common-english-words.txt
.. [#] http://www.ics.uci.edu/~newman/pubs/fastlda.pdf
.. [#] http://machinelearning.wustl.edu/mlpapers/paper_files/NIPS2006_511.pdf
.. [#] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf

