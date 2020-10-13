const SOURCE='D:\\wikidata\\latest-all.json\\latest-all.json'

let TASK = process.argv[2]


const fs = require( 'fs' )
const readline = require( 'readline' )
const stream = require( 'stream' )

const tasks = {

    'property_stats': {

        clusterable: false,
        start(){
            this.properties = {}
        },
        step( json ){
        
            if( Object.keys( json.labels ).includes('fr') && Object.keys( json.descriptions ).includes('fr') ){
                if( json.type == 'item' ){
                    Object.keys( json.claims ).forEach( (property) => {
                        if( this.properties[ property ] == null ){
                            this.properties[ property ] = { id: property , count: 0 }
                        }
                        this.properties[ property ].count += 1
                    })
                } else if( json.type == 'property' ){
                    if( this.properties[ json.id ] == null ){
                        this.properties[ json.id ] = { id: json.id , count: 0 }
                    }
                    this.properties[ json.id ].label = json.labels.fr.value
                    this.properties[ json.id ].description = json.descriptions.fr.value     
                }
			}
			
        },
        end(){
			console.log( JSON.stringify(this.properties) )

			// CSV EXPORT
			// Object.values( this.properties ).forEach( property => {
			// 	console.log( property.id + ';' + property.count + ';' + ( property.label ? property.label : '' ) + ';' + ( property.description ? property.description : '' ) )
			// })

        }
    },


    'select_lang': {
        clusterable: true,
        start(){
            this.frProperties = Object.keys( require( './data.properties.fr.001.json' ) )
            console.log( '[' )
        },
        step( json ){

            if( Object.keys( json.labels ).includes('fr') && Object.keys( json.descriptions ).includes('fr') ){

                json.labels = { fr: json.labels.fr }
                json.descriptions = { fr: json.descriptions.fr }
                json.aliases = { fr: json.aliases.fr }
            
                if( json.type == 'item' ){
                    json.claims = Object.keys( json.claims )
                                    .filter( c => this.frProperties.includes(c) )
                                    .reduce( (result,propertyId) => {
                                        let claim = json.claims[ propertyId ]

                                        

                                        result[ propertyId ] = claim
                                        return result
                                    } , {} )
                }
        
                console.log( JSON.stringify(json) + ',' )
            }        

        },
        end(){
            console.log( ']' )
        }
    }



}



            // let result = Object.assign( json , {
            //     labels: { fr: json.labels.fr },
            //     descriptions: { fr: json.descriptions.fr },
            //     aliases: { fr: json.aliases.fr }
            // } )
            //let resultLine = JSON.stringify( result )

            //let resultLine = '"' + json.id + '": "' + json.labels.fr.value + '",'


if( TASK == null || TASK.trim().length == 0 || ! Object.keys(tasks).includes(TASK) ){
    console.error( 'bad input' , TASK )
    process.exit(1)
}

let inStream = fs.createReadStream( SOURCE );
let outStream = new stream();
let rl = readline.createInterface(inStream, outStream);

let lineCount = 0
let scanSize = 0
let scanStart = new Date()

console.error( 'scan start' , scanStart.toString() )
tasks[ TASK ].start()

rl.on( 'SIGINT' , function() {
    console.error( 'LINE COUNT' , lineCount )
})

rl.on('pause', function() {
    console.error( 'READLINE PAUSED' )
})

rl.on('close', function() {
    //console.log( '}' )
	console.error( 'END OF FILE' )
	tasks[ TASK ].end()
    let scanEnd = new Date()
    console.error( scanEnd.toString() , scanEnd - scanStart )
    console.error( 'line count' , lineCount )
    console.error( 'scan size' , scanSize )
})

rl.on( 'line' , function( line ) {

    if( line == '[' || line == ']' ){ return }

    if( scanSize > 1 * 1024 * 1024 * 1024 ){
        console.error( 'max scanSize, close' , scanSize )
        rl.close()
	}
	
    try{
        let json = JSON.parse( line.slice( 0 , line.length - 1 ) )

		tasks[ TASK ].step( json )

    }catch( e ){
        console.error( 'JSON PARSE ERROR' , e.message )
    }

    scanSize += line.length
    lineCount++
});
