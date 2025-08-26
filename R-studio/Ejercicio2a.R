# load the library first
library(caret)


# load the data
data <- read.csv("~/BigDataProject/R-studio/ficheros_entrada/prediccion.txt", header = FALSE, dec = ".", sep = ",", nrows = 30)

# Rename two columns
colnames(data)[1] <- "date"
colnames(data)[146] <- "nextAverage"

set.seed(10)
inTrain <- createDataPartition(data$nextAverage, p = 0.7, list = FALSE)
training <- data[ inTrain,]
testing <- data[-inTrain,]


nnetFit <- train(nextAverage ~ .
                  , data = training, method = "nnet")

pred <-predict(nnetFit,testing)

postResample(pred, testing$nextAverage)
