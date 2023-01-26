const util = require('./util')
const Generator = require('./generator')
const Combinator = require('./combinator')
const Cleaner = require('./cleaner')

class AutoIndex {
    
    constructor() {}

    autoindex(inputDir, outputDir=null, minimize=false, verbose=true) {

        if (verbose) console.log(" - Reading input directory: " + inputDir)

        let queries = util.getAggregationsFromDir(inputDir, "default")
        if (verbose) console.log("\t * Got " + queries.length + " queries")

        const generator = new Generator()
        let indexResult = generator.generate(queries)

        let totalStepIndexes = 0
        let combinedIndexes = []
    
        for (const [key, indexes] of Object.entries(indexResult)) {
        
            if (verbose) console.log(" - " + key +" (" + indexes.length + " indexes): ")
            totalStepIndexes += indexes.length
            for (let index of indexes){
                if (verbose) console.log("\t * ", JSON.stringify(index.toMongoJSON()))
            }

            const combinator = new Combinator(minimize)
            let aggregationIndexes = combinator.combine(indexResult[key])
            combinedIndexes = combinedIndexes.concat(aggregationIndexes)
            if (verbose) console.log("\t => Combined: ", aggregationIndexes.map(i => i.key))
        }

        const cleaner = new Cleaner(minimize)
        let allIndexes = cleaner.clean(combinedIndexes)

        if (verbose) {
            console.log("\t => Cleaned: ", allIndexes.map(i => i.collection + ' ' + JSON.stringify(i.key)))
            console.log("Total Step Indexes: ", totalStepIndexes)
            console.log("Total Combined Indexes: ", combinedIndexes.length)
            console.log("Total Final Indexes: ", allIndexes.length)
        }

        if (outputDir) {
            let indexResults = util.writeIndexResults(allIndexes, outputDir)
            if (verbose) console.log("\t => Written Results to: ", outputDir, '\n Results: \n', indexResults)
        }
        return(allIndexes)
    }
}

module.exports = AutoIndex