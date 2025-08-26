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

# Renombramos la última columna de los nuevos dataset
colnames(training)[1] <- "date"
colnames(training)[146] <- "nextAverage"
colnames(testing)[1] <- "date"
colnames(testing)[146] <- "nextAverage"

# Normalizar los datos (excluyendo la fecha)
preProc <- preProcess(training[, 2:146], method = "range")

# Aplicar la normalización a los conjuntos de entrenamiento y de test
trainingNorm <- predict(preProc, training[, 2:146])
testingNorm <- predict(preProc, testing[, 2:146])

# Renombramos la última columna de los nuevos dataset
colnames(trainingNorm)[145] <- "nextAverage"
colnames(testingNorm)[145] <- "nextAverage"

# Entrenar el modelo con datos normalizados
nnetFit <- train(nextAverage ~ ., data = trainingNorm, method = "nnet", linout = TRUE)

# Predecir con datos normalizados
pred <- predict(nnetFit, testingNorm)

# Comparar resultados
resultado <- data.frame(
  Real = testingNorm$nextAverage,
  Prediccion = as.numeric(pred)
)

head(resultado)

min_target <- min(training$nextAverage)
max_target <- max(training$nextAverage)

resultado$Prediccion_real <- resultado$Prediccion * (max_target - min_target) + min_target
resultado$Real_original <- resultado$Real * (max_target - min_target) + min_target

View(resultado[, c("Real_original", "Prediccion_real")])