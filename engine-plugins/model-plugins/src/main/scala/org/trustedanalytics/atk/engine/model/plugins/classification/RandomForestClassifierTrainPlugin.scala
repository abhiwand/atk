////////////////////////////////////////////////////////////////////////////////
//// INTEL CONFIDENTIAL
////
//// Copyright 2015 Intel Corporation All Rights Reserved.
////
//// The source code contained or described herein and all documents related to
//// the source code (Material) are owned by Intel Corporation or its suppliers
//// or licensors. Title to the Material remains with Intel Corporation or its
//// suppliers and licensors. The Material may contain trade secrets and
//// proprietary and confidential information of Intel Corporation and its
//// suppliers and licensors, and is protected by worldwide copyright and trade
//// secret laws and treaty provisions. No part of the Material may be used,
//// copied, reproduced, modified, published, uploaded, posted, transmitted,
//// distributed, or disclosed in any way without Intel's prior express written
//// permission.
////
//// No license under any patent, copyright, trade secret or other intellectual
//// property right is granted to or conferred upon you by disclosure or
//// delivery of the Materials, either expressly, by implication, inducement,
//// estoppel or otherwise. Any license under such intellectual property rights
//// must be express and approved by Intel in writing.
////////////////////////////////////////////////////////////////////////////////
//
//package org.trustedanalytics.atk.engine.model.plugins.classification
//
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.tree.RandomForest
//import org.apache.spark.rdd.RDD
//import org.trustedanalytics.atk.UnitReturn
//import org.trustedanalytics.atk.domain.frame.FrameReference
//import org.trustedanalytics.atk.domain.model.ModelReference
//import org.trustedanalytics.atk.engine.frame.SparkFrame
//import org.trustedanalytics.atk.engine.model.Model
//import org.trustedanalytics.atk.engine.model.plugins.FrameRddImplicits._
//import org.trustedanalytics.atk.engine.plugin.{ApiMaturityTag, Invocation, SparkCommandPlugin}
//
////Implicits needed for JSON conversion
//import spray.json._
//
//case class RandomForestClassifierTrainArgs(model:ModelReference,
//                                 frame: FrameReference,
//                                 labelColumn: String,
//                                 observationColumns: List[String],
//                                 numClasses: Option[Int] = None,
//                                 categoricalFeaturesInfo: Option[Map[Int,Int]] = None,
//                                 numTrees: Option[Int] = None,
//                                 featureSubsetCategory: Option[String] = None,
//                                 impurity: Option[String] = None,
//                                 maxDepth: Option[Int]= None,
//                                 maxBins: Option[Int] = None,
//                                 seed: Option[Int] = None) {
//require(model != null, "model is required")
//require(frame != null, "frame is required")
//require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
//require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
//
//
//  def getNumClasses: Int = {
//    if (numClasses.isDefined) { require(numClasses.get > 1, "numClasses must be atleast 2") }
//    numClasses.getOrElse(2)
//  }
//
//  def getCategoricalFeaturesInfo : Map[Int,Int] = {
//    categoricalFeaturesInfo.getOrElse(Map[Int,Int]())
//  }
//
//  //TODO: Verify default value
//  def getNumTrees : Int = {
//    numTrees.getOrElse(1)
//  }
//
//  def getFeatureSubsetCategory : String = {
//      featureSubsetCategory.getOrElse("all") match {
//        case "auto" => {
//          numTrees.getOrElse(1) match {
//            case 1 => "all"
//            case other => "sqrt"
//          }
//      }}}
//
//  def getImpurity : String = {
//    impurity.getOrElse("gini")
//  }
//
//  def getMaxDepth : Int = {
//    maxDepth.getOrElse(4)
//  }
//
//  def getMaxBins : Int = {
//    maxBins.getOrElse(100)
//  }
//
//  def getSeed : Int = {
//    maxBins.getOrElse(scala.util.Random.nextInt())
//  }
//}
//
//class RandomForestClassifierTrainPlugin extends SparkCommandPlugin[RandomForestClassifierTrainArgs, UnitReturn] {
//  /**
//   * The name of the command.
//   *
//   * The format of the name determines how the plugin gets "installed" in the client layer
//   * e.g Python client via code generation.
//   */
//  override def name: String = "model:random_forest/train_classifier"
//
//  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)
//
//  /**
//   * Number of Spark jobs that get created by running this command
//   * (this configuration is used to prevent multiple progress bars in Python client)
//   */
//  override def numberOfJobs(arguments: RandomForestClassifierTrainArgs)(implicit invocation: Invocation) = 109
//
//  /**
//   * Run MLLib's NaiveBayes() on the training frame and create a Model for it.
//   *
//   * @param invocation information about the user and the circumstances at the time of the call,
//   *                   as well as a function that can be called to produce a SparkContext that
//   *                   can be used during this invocation.
//   * @param arguments user supplied arguments to running this plugin
//   * @return a value of type declared as the Return type.
//   */
//  override def execute(arguments: RandomForestClassifierTrainArgs)(implicit invocation: Invocation): UnitReturn = {
//    val frame: SparkFrame = arguments.frame
//    val model: Model = arguments.model
//
//    //create RDD from the frame
//    val labeledTrainRdd: RDD[LabeledPoint] = frame.rdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)
//    val randomForestModel = RandomForest.trainClassifier(labeledTrainRdd, arguments.getNumClasses, arguments.getCategoricalFeaturesInfo, arguments.getNumTrees,
//      arguments.getFeatureSubsetCategory, arguments.getImpurity, arguments.getMaxDepth, arguments.getMaxBins, arguments.getSeed)
//    val jsonModel = new RandomForestClassifierData(randomForestModel, arguments.observationColumns, arguments.getNumClasses)
//
//    model.data = jsonModel.toJson.asJsObject
//  }
//}
