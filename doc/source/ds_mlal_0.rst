===========================
Machine Learning Algorithms
===========================

.. contents:: Table of Contents
    :local:
    
.. toctree::
    :hidden:
    
    ds_mlal_cf
    ds_mlal_als
    ds_mlal_cgd
    ds_mlal_pr
    ds_mlal_lp
    ds_mlal_lbp
    ds_mlal_glbp
    ds_mlal_lda
    ds_mlal_apl
    ds_mlal_cc
    ds_mlal_k

The graph machine learning algorithms currently supported fall into these categories:
*Collaborative Filtering*, *Graph Analytics*, *Graphical Models*, and *Topic Modeling*.

* :ref:`Collaborative_Filtering`
    * :ref:`ALS`
    * :ref:`CGD`

* :ref:`Graph_Analytics`
    * :ref:`CC`
    * :ref:`PR`

.. TODO::

    * ref:`APL`
    
    Add these to the toctree above.


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

The algorithms we currently support in this category :ref:`CC` and :ref:`PR`.


.. _CC:
.. include:: ds_mlal_cc.rst

.. _PR:
.. include:: ds_mlal_pr.rst

.. TODO::

    We support three algorithms in this category, ref:`APL`, ref:`CC`, and :ref:`PR`

    .. _APL:
    .. include:: ds_mlal_apl.rst

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
