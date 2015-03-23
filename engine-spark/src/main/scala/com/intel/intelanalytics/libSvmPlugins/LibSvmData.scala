//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.libSvmPlugins

import libsvm.svm_model


/**
 * Command for loading model data into existing model in the model database.
 * @param svmModel Trained libSVMModel object
 * @param observationColumns Handle to the observation columns of the data frame
 */
case class LibSvmData(svmModel: svm_model, observationColumns: List[String]) {
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
  require(svmModel != null, "libsvmModel must not be null")
}

case class SvmModel(svCoef: Array[Array[Double]]) {

  def this(svmModel: svm_model) = {
    this(svmModel.sv_coef)
  }

}

/*
public class svm_model implements java.io.Serializable
{
public svm_parameter param;	// parameter
public int nr_class;		// number of classes, = 2 in regression/one class svm
public int l;			// total #SV
public svm_node[][] SV;	// SVs (SV[l])
public double[][] sv_coef;	// coefficients for SVs in decision functions (sv_coef[k-1][l])
public double[] rho;		// constants in decision functions (rho[k*(k-1)/2])
public double[] probA;         // pariwise probability information
public double[] probB;
public int[] sv_indices;       // sv_indices[0,...,nSV-1] are values in [1,...,num_traning_data] to indicate SVs in the training set

// for classification only

public int[] label;		// label of each class (label[k])
public int[] nSV;		// number of SVs for each class (nSV[k])
// nSV[0] + nSV[1] + ... + nSV[k-1] = l
};

 */ 