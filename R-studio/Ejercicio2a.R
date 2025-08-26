# ------------------------------------------------------------------------------
# jsgifbec
# Bigdata: Proyecto - Parte 2a
# ------------------------------------------------------------------------------

# load the libraries first
library(caret)
library(randomForest)

set.seed(10)

# Cargamos el dataset desde el fichero
data <- read.csv("~/BigDataProject/R-studio/ficheros_entrada/prediccion.txt", header = FALSE, dec = ".", sep = ",", nrows = 30)

# Rename two columns
colnames(data)[1] <- "date"
colnames(data)[146] <- "nextAverage"


inTrain <- createDataPartition(data$nextAverage, p = 0.7, list = FALSE)
training <- data[ inTrain,]
testing <- data[-inTrain,]

# Aplicar la normalización a los conjuntos de entrenamiento y de test
preProc <- preProcess(training[, 2:146], method = "range")
trainingNorm <- predict(preProc, training[, 2:146])
testingNorm <- predict(preProc, testing[, 2:146])

# Renombramos la última columna de los nuevos dataset
colnames(training)[1] <- "date"
colnames(training)[146] <- "nextAverage"
colnames(testing)[1] <- "date"
colnames(testing)[146] <- "nextAverage"
colnames(trainingNorm)[145] <- "nextAverage"
colnames(testingNorm)[145] <- "nextAverage"

# calculamos los valores mín y max usados en la normalización
min_target <- min(training$nextAverage)
max_target <- max(training$nextAverage)

# 1. Neuronal Network sin normalizar
nNFit <- train(nextAverage ~ ., data = training[, 2:146], method = "nnet", linout = TRUE, trace = FALSE)
               #, tuneGrid = expand.grid(size = c(5, 10), decay = c(0.1, 0.5)))

nNpred <- predict(nNFit, testing)

resultado <- data.frame(
  Real = testing$nextAverage,
  PrediccionSinNorm = as.numeric(nNpred)
)

# 2. Neuronal Network con normalización
nNFitNorm <- train(nextAverage ~ ., data = trainingNorm, method = "nnet", linout = TRUE, trace = FALSE)
                   #, tuneGrid = expand.grid(size = c(5, 10), decay = c(0.1, 0.5)))
nNpredNorm <- predict(nNFitNorm, testingNorm)

resultadoNorm <- data.frame(
  PrediccionNorm = as.numeric(nNpredNorm)
)

resultado$PrediccionConNorm <- resultadoNorm$Prediccion * (max_target - min_target) + min_target

# 3. Random Forest sin normalización
rfFit <- train(nextAverage ~ ., data = training[, 2:146], method = "rf")
rFpred <- predict(rfFit, testing[, 2:146])
resultado$PrediccionRandomForestSinNorm <- as.numeric(rFpred)

# 4. Random Forest con normalización
rfFitNorm <- train(nextAverage ~ ., data = trainingNorm, method = "rf")
rFpredNorm <- predict(rfFitNorm, testingNorm)
rFpredNorm <- rFpredNorm * (max_target - min_target) + min_target
resultado$PrediccionRandomForestConNorm <- as.numeric(rFpredNorm)

# 5. Visualización de los datos
View(resultado[, c("Real", "PrediccionSinNorm", "PrediccionConNorm", "PrediccionRandomForestSinNorm", "PrediccionRandomForestConNorm")])