const handleMessage = require( './common.js' ).handleMessage

module.exports = {

    clusterable: true,
    isFirst: true,
    handleMessage,
    start(){
        this.isFirst = true
        this.frProperties = Object.keys( require( '../property_stats.json' ) )
        console.log( '[' )
    },
    step( json ){

        if( Object.keys( json.labels ).includes('fr') && Object.keys( json.descriptions ).includes('fr') ){

            json.labels = { fr: json.labels.fr }
            json.descriptions = { fr: json.descriptions.fr }
            json.aliases = { fr: json.aliases.fr }

            filterKeys( json , 'claims' , this.frProperties , null , (claim) => {

                if( claim.mainsnak ){
                    if( claim.mainsnak.datavalue && claim.mainsnak.datavalue.value && claim.mainsnak.datavalue.value.language != 'fr' ){
                        delete claim.mainsnak
                    } 
                }

                if( claim.references ){
                    claim.references.forEach( reference => {
                        if( reference.snaks ){
                            filterKeys( reference , 'snaks' , this.frProperties , 'snaks-order' , (item) => {
                                if( item.datavalue && item.datavalue.value && item.datavalue.value.language != 'fr' ){
                                    return false
                                } else {
                                    return true
                                }
                            } )
                        }
                    })
                }

                if( claim.qualifiers ){
                    filterKeys( claim , 'qualifiers' , this.frProperties , 'qualifiers-order' )
                }

                return true
            })

            if( json.sitelinks ){
                filterKeys( json , 'sitelinks' , 'frwiki' )
            }

            console.log( ( this.isFirst ? '' : ', ' ) + JSON.stringify(json) )
            this.isFirst = false
        }        

    },
    end(){
        console.log( ']' )
    }
}


const filterKeys = ( target , targetProperty , only , listProperty , filterValue ) => {

    target[ targetProperty ] = Object.keys(target[ targetProperty ]).filter( key => {

        if( typeof only == 'string' && only == key ){
            return true
        } else if( Array.isArray(only) && only.includes(key) ){
            return true
        } else {
            if( listProperty ){
                target[listProperty].remove( key )
            }
            return false
        }

    } ).reduce( ( values , key ) => {
        if( filterValue == null ){
            values[ key ] = target[ targetProperty ][ key ]

        } else {
            let items = target[ targetProperty ][ key ].reduce( (result,item) => {
                if( filterValue(item) ){
                    result.push(item)
                } else {
                    if( listProperty ){
                        target[listProperty].remove( key )
                    }    
                }
                return result
            } , [] )

            if( items.length > 0 ){
                values[ key ] = items
            }

        }
        return values

    } , {} )

}

Array.prototype.remove = function( element ){
    let index = this.indexOf(element)
    if( index >= 0 ){
        this.splice( index , 1 )
    }
}
