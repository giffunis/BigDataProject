# load the library first
library(caret)

set.seed(10)

# load the data
data <- read.csv("~/BigDataProject/R-studio/ficheros_entrada/prediccion.txt"
                     , header = FALSE, dec = ".", sep = ",", nrows = 30)

measures <- data[, 2:145]
nextAverage <- data[, 146]

dataFull <- cbind(measures, nextAverage)

colnames(dataFull)[145] <- "nextAverage"


inTrain <- createDataPartition(dataFull$nextAverage, p = 0.7, list = FALSE)


training <- dataFull[ inTrain,]
testing <- dataFull[-inTrain,]

nnetFit <- train(nextAverage ~ ., data = training, method = "nnet")

pred <-predict(nnetFit,testing)
View(pred)

confusionMatrix(data = pred, testing$nextAverage)
