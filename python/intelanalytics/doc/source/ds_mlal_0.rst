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
.. include:: ds_mlal_cf.rst

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

| 

<- :doc:`ds_ml`
|-------------------------------|
:doc:`ds_apic` ->

<- :doc:`index`

.. rubric:: footnotes

.. [#LP1] http://www.cs.cmu.edu/~zhuxj/pub/CMU-CALD-02-107.pdf
.. [#LDA1] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf
.. [#LDA2] http://www.sciencedirect.com/science/article/pii/S1532046401910299
.. [#LDA3] http://tartarus.org/~martin/PorterStemmer/index.html
.. [#LDA4] http://www.textfixer.com/resources/common-english-words.txt
.. [#LDA5] http://www.ics.uci.edu/~newman/pubs/fastlda.pdf
.. [#LDA6] http://machinelearning.wustl.edu/mlpapers/paper_files/NIPS2006_511.pdf
.. [#LDA7] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf

