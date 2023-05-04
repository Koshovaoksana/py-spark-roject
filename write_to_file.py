
def writeToFile(df, directory_to_write):
    df.write.csv(directory_to_write, header=True, )
