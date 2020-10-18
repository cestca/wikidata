const { type } = require('os')

module.exports = {

    'property_stats': {
        clusterable: false,
        initProperty( property ){
            if( this.properties == null ){
                this.properties = {}
            }
            if( this.properties[ property ] == null ){
                this.properties[ property ] = { id: property , count: 0 }
            }
        },
        incrementProperty( { property } ){
            this.initProperty( property )
            this.properties[ property ].count += 1
        },
        setPropertyDetails( { property , label , description } ){
            this.initProperty( property )
            this.properties[ property ].label = label
            this.properties[ property ].description = description     
        },
        step( json , sendMessage ){
        
            if( Object.keys( json.labels ).includes('fr') && Object.keys( json.descriptions ).includes('fr') ){
                if( json.type == 'item' ){
                    Object.keys( json.claims ).forEach( (property) => {
                        sendMessage( { method: 'incrementProperty' , property } )
                        //this.incrementProperty( property )
                        json.claims[ property ].forEach( item => {
                            if( item.qualifiers ){
                                Object.keys( item.qualifiers ).forEach( q => {
                                    sendMessage( { method: 'incrementProperty' , property: q } )
                                    //this.incrementProperty( q )
                                } )
                            }
                            if( item.references ){
                                item.references.forEach( reference => {
                                    Object.keys( reference.snaks ).forEach( s => {
                                        sendMessage( { method: 'incrementProperty' , property: s } )
                                        //this.incrementProperty( s )
                                    })
                                } )
                            }
                        })
                    })
                } else if( json.type == 'property' ){

                    sendMessage( {
                        method: 'setPropertyDetails',
                        property: json.id,
                        label: json.labels.fr.value,
                        description: json.descriptions.fr.value
                    } )

                    //this.setPropertyDetails( json.id , json.labels.fr.value , json.descriptions.fr.value )
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
        isFirst: true,
        start(){
            this.isFirst = true
            this.frProperties = Object.keys( require( './property_stats.json' ) )
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
