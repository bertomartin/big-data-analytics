# Big Data Analytics - final project
## Overview
The aim of this project is to build a model that predicts whether a company will beat consensus estimates when they report earnings.

This information can then be used as the input to a trading system. It can also be used to gain a better insight into a company's earnings, maybe as a first step to further research.

## Data
We download OHLC(V) data from Yahoo. We gather earnings data from both Estimize and Quantdl/Zack's. 

## Data Processing
Data processing involved modifying the format of the downloaded data, moving it through a pipeline so to speak, so that eventually we can generate features that could be used to train our classifier. At this point, we also needed to join the data from Yahoo with the data from Estimize/Zacks.

## Feature Selection
The features were mainly hand selected. Based on our experience and ideas about the markets, we generated features based on moving averages of prices, price momentums and volume momentum. We hope to add more features, and specifically auto-generated features so we can compare our model outputs. The features are the key to any ML project, and there isn't a pre-set feature set for this type of work (as opposed to Bag of Words in text analytics).

## Model Development

## Model Evaluation
