# load the library first
library(caret)
set.seed(10)

# Cargamos el dataset desde el fichero
data <- read.csv("~/BigDataProject/R-studio/ficheros_entrada/prediccion.txt", header = FALSE, dec = ".", sep = ",", nrows = 30)

# Rename two columns
colnames(data)[1] <- "date"
colnames(data)[146] <- "nextAverage"


inTrain <- createDataPartition(data$nextAverage, p = 0.7, list = FALSE)
training <- data[ inTrain,]
testing <- data[-inTrain,]

# Renombramos la última columna de los nuevos dataset
colnames(training)[1] <- "date"
colnames(training)[146] <- "nextAverage"
colnames(testing)[1] <- "date"
colnames(testing)[146] <- "nextAverage"

# 1. Sin normalizar
# 1.1 Entrenar el modelo con datos sin normalizar
nnetFit <- train(nextAverage ~ ., data = training[, 2:146], method = "nnet", linout = TRUE)

# 1.2 Predecimos
pred <- predict(nnetFit, testing)

# 1.3.Guardamos el resultado
resultado <- data.frame(
  Real = testing$nextAverage,
  PrediccionSinNorm = as.numeric(pred)
)

# 2. Con normalización

# 2.1 Normalizar los datos (excluyendo la fecha)
preProc <- preProcess(training[, 2:146], method = "range")

# Aplicar la normalización a los conjuntos de entrenamiento y de test
trainingNorm <- predict(preProc, training[, 2:146])
testingNorm <- predict(preProc, testing[, 2:146])

# Renombramos la última columna de los nuevos dataset
colnames(trainingNorm)[145] <- "nextAverage"
colnames(testingNorm)[145] <- "nextAverage"

# 2.2 Entrenar el modelo con datos normalizados
nnetFit <- train(nextAverage ~ ., data = trainingNorm, method = "nnet", linout = TRUE)

# 2.3 Predecimos
pred <- predict(nnetFit, testingNorm)

# 2.4.Guardamos el resultado
resultadoNorm <- data.frame(
  Real = testing$nextAverage,
  PrediccionNorm = as.numeric(pred)
)

min_target <- min(training$nextAverage)
max_target <- max(training$nextAverage)

resultado$PrediccionConNorm <- resultadoNorm$Prediccion * (max_target - min_target) + min_target


View(resultado[, c("Real", "PrediccionSinNorm", "PrediccionConNorm")])