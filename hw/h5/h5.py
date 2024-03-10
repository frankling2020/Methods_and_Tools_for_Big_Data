from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA as PCAml, VectorAssembler
from pyspark.ml.regression import LinearRegression

############################################################################
##                             Prepare                                    ##
############################################################################

appName = 'h5'
master = 'local[2]'

ss = SparkSession.builder.appName(appName).master(master).getOrCreate()
sc = ss.sparkContext

inFile = "file:///root/code/sensors1.csv"
resFile = '/root/code/res.txt'

df1 = ss.read.csv(inFile, header=False, inferSchema=True)


############################################################################
##                             Ex 3.2                                     ##
############################################################################

va = VectorAssembler(inputCols=df1.columns[:-1], outputCol="features")
df1 = va.transform(df1)

pca = PCAml(k=3, inputCol="features", outputCol='pca_features')
model = pca.fit(df1)
df1 = model.transform(df1)


############################################################################
##                             Ex 3.3                                     ##
############################################################################

lin_reg = LinearRegression(featuresCol="pca_features", labelCol='_c1000')
df1_sel = df1.select('pca_features', '_c1000')
lm = lin_reg.fit(df1_sel)


############################################################################
##                             Ex 3.4                                     ##
############################################################################
inFile2 = "file:///root/code/sensors2.csv"

df2 = ss.read.csv(inFile2, header=False, inferSchema=True)
va2 = VectorAssembler(inputCols=df2.columns[:-1], outputCol="features")
df2 = va2.transform(df2)
pca2 = PCAml(k=3, inputCol="features", outputCol='pca_features')
model2 = pca2.fit(df2)
df2 = model2.transform(df2)

lin_reg2 = LinearRegression(featuresCol="pca_features", labelCol='_c1000')
df2_sel = df2.select('pca_features', '_c1000')
lm2 = lin_reg.fit(df2_sel)

res = lm2.evaluate(df2_sel)


############################################################################
##                             Summary                                    ##
############################################################################

with open(resFile, 'w+') as out:
    out.write("Ex 3.2 \n")
    for var in model.explainedVariance.cumsum():
        out.write(str(var)+"\n") 
    out.write("\n")
    
    out.write("Ex 3.3 \n")
    for coeff in lm.coefficients:
        out.write(str(coeff)+"\n")
    out.write(str(lm.intercept)+"\n")
    out.write("\n")
    
    out.write("Ex 3.4 \n")
    for coeff in lm2.coefficients:
        out.write(str(coeff)+"\n")
    out.write(str(lm2.intercept)+"\n")
    out.write("R^2: " + str(res.r2)+"\n")