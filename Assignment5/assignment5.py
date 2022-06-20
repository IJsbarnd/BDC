#!/usr/local/bin/python3

""" Assignment 5"""

__author__ = "IJsbrand Pool"
__version__ = "2.0"

import os
import sys
from csv import writer
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, desc, split, col, explode
from pyspark.sql.types import IntegerType


class InterproTsvParser():
    """ Interpro tsv file parser, answers questions 1 to 10. """

    def __init__(self, my_object) -> None:
        self.my_object = my_object
        self.result = []
        self.explanation = []

    def question_one(self):
        """ Question one: How many distinct protein annotations are found in the dataset?
            I.e. how many distinc InterPRO numbers are there? """

        unique_interpro = self.my_object.select("_c11").distinct().count()
        explain_question_one = self.my_object._jdf.queryExecution().toString()

        self.result.append(unique_interpro)
        self.explanation.append(explain_question_one)

        return unique_interpro, explain_question_one

    def question_two(self):
        """ Question two: How many annotations does a protein have on average? """

        collected = self.my_object.groupBy("_c0").count().select(mean("count")).collect()
        annotations = collected[0][0]
        explain_question_two = self.my_object._jdf.queryExecution().toString()

        self.result.append(annotations)
        self.explanation.append(explain_question_two)

        return annotations, explain_question_two

    def question_three(self):
        """ Question three: What is the most common GO Term found? """

        ex_object = self.my_object.withColumn('_c13', explode(split("_c13", "[|]")))
        group_object = ex_object.groupby("_c13").count()
        most_common = group_object.orderBy(desc('count')).take(2)[1][0]
        explain_question_three = group_object._jdf.queryExecution().toString()

        self.result.append(most_common)
        self.explanation.append(explain_question_three)

        return most_common, explain_question_three

    def question_four(self):
        """ Question four: What is the average size of an InterPRO feature found in the dataset? """

        length_avg = self.my_object.select(mean("_c2")).collect()
        explain_question_four = self.my_object._jdf.queryExecution().toString()

        self.result.append(length_avg[0][0])
        self.explanation.append(explain_question_four)

        return length_avg[0][0], explain_question_four

    def question_five(self, my_object):
        """ Question five: What is the top 10 most common InterPRO features? """

        grouped_object = my_object.groupby("_c11").count()
        most_common = grouped_object.orderBy(desc('count')).take(11)

        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explain_question_five = grouped_object._jdf.queryExecution().toString()

        self.result.append(top_10)
        self.explanation.append(explain_question_five)

        return top_10, explain_question_five

    def question_six(self):
        """ Question six: If you select InterPRO features that are almost the same size
            (within 90-100%) as the protein itself, what is the top10 then? """

        length_object = self.my_object.withColumn("Length", self.my_object['_c7'] -
                                                  self.my_object['_c6'] + 1)

        range_object = length_object.withColumn("Bottom_range", length_object["_c2"] * 0.9)

        similar_object = range_object.filter(
            (range_object["Length"] >= range_object["Bottom_range"]) & (range_object["Length"]
                                                                        < range_object["_c2"]))
        return self.question_five(similar_object)

    def question_seven(self):
        """ Question seven: If you look at those features which also have textual annotation,
            what is the top 10 most common word found in that annotation? """

        exploded_object = self.my_object.withColumn('_c12', explode(split("_c12", " ")))
        grouped_object = exploded_object.groupby("_c12").count()
        most_common = grouped_object.orderBy(desc('count')).take(11)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explain_question_seven = grouped_object._jdf.queryExecution().toString()

        self.result.append(top_10)
        self.explanation.append(explain_question_seven)
        return top_10, explain_question_seven

    def question_eight(self):
        """ Question eight: And the top 10 least common? """

        exploded_object = self.my_object.withColumn('_c12', explode(split("_c12", " ")))
        grouped_object = exploded_object.groupby("_c12").count()
        most_common = grouped_object.orderBy(col("count").asc()).take(10)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explain_question_eight = grouped_object._jdf.queryExecution().toString()

        self.result.append(top_10)
        self.explanation.append(explain_question_eight)

        return top_10, explain_question_eight

    def question_nine(self):
        """ Question nine Combining your answers for Q6 and Q7,
            what are the 10 most commons words found for the largest InterPRO features? """

        length_object = self.my_object.withColumn("Lenght", self.my_object['_c7']
                                                  - self.my_object['_c6'] + 1)

        range_object = length_object.withColumn("bottom_range", length_object["_c2"] * 0.9)

        similar_object = range_object.filter(
            (range_object["Lenght"] >= range_object["bottom_range"]) & (range_object["Lenght"]
                                                                        < range_object["_c2"]))

        exploded_object = similar_object.withColumn('_c12', explode(split("_c12", " ")))
        grouped_object = exploded_object.groupby("_c12").count()
        most_common = grouped_object.orderBy(desc('count')).take(11)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explain_question_nine = grouped_object._jdf.queryExecution().toString()

        self.result.append(top_10)
        self.explanation.append(explain_question_nine)
        return top_10, explain_question_nine

    def question_ten(self):
        """ Question ten: What is the coefficient of correlation ($R^2$)
            between the size of the protein and the number of features found? """

        sub_object = self.my_object.select(["_c0", "_c2", "_c11"])
        sub_object = sub_object.filter((self.my_object["_c11"] != None)
                                             | (self.my_object["_c11"] != "-"))
        sub_object = sub_object.groupBy("_c0", "_c2").count()
        sub_object = sub_object.withColumn("_c2", sub_object["_c2"].cast(IntegerType()))
        sub_object = sub_object.withColumn("count", sub_object["count"].cast(IntegerType()))
        explain_question_ten = sub_object._jdf.queryExecution().toString()
        mynewsubset = sub_object.stat.corr("_c2", "count")

        self.result.append(mynewsubset)
        self.explanation.append(explain_question_ten)
        print(mynewsubset)

        return mynewsubset, explain_question_ten

    def write_interpro_csv(self):
        """ Writes to csv. """

        print(self.result)
        with open('results.csv', 'a', newline='') as f_object:
            for i, result, explanation in zip([i for i in range(1, 11)],
                                              self.result, self.explanation):
                writer_object = writer(f_object)
                writer_object.writerow([i, result, explanation])

            f_object.close()


def main(args):
    """ Main function. """

    SparkContext('local[16]')

    print("Started process.")
    spark = SparkSession \
        .builder \
        .appName("csv reader") \
        .getOrCreate()

    path = args[1]
    print(os.getcwd())
    my_object = spark.read.csv(path, sep='\t', header=False)
    mypars = InterproTsvParser(my_object)
    my_object.show()
    _, _, _, _, _, _, _, _, _, _ = mypars.question_one(), mypars.question_two(), \
                                   mypars.question_three(), mypars.question_four(), \
                                   mypars.question_five(my_object), mypars.question_six(), \
                                   mypars.question_seven(), mypars.question_eight(), \
                                   mypars.question_nine(), mypars.question_ten()
    # Write outputs to csv
    mypars.write_interpro_csv()


if __name__ == "__main__":
    main(sys.argv)