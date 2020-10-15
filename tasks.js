
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
        start(){
            this.frProperties = Object.keys( require( './property_stats.json' ) )
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
                                    .reduce( (claims,propertyId) => {

                                        json.claims[ propertyId ].forEach( claim => {

                                            if( claim.references ){

                                                claim.references.forEach( reference => {

                                                    if( reference.snaks ){
                                                        reference.snaks = Object.keys(reference.snaks)
                                                                            .filter( s => {
                                                                                if( this.frProperties.includes(s) ){
                                                                                    return true
                                                                                } else {
                                                                                    reference['snaks-order'].splice( reference['snaks-order'].indexOf(s) , 1 )
                                                                                    return false
                                                                                }
                                                                            } )
                                                                            .reduce( (snaks,snakId) => {
                                                                                reference.snaks[snakId] = reference.snaks[snakId].filter( snack => {
                                                                                    if( snack.datavalue && snack.datavalue.value && snack.datavalue.value.language != 'fr' ){
                                                                                        reference['snaks-order'].splice( reference['snaks-order'].indexOf(snakId) , 1 )
                                                                                        return false
                                                                                    } else {
                                                                                        return true
                                                                                    }
                                                                                })

                                                                                if( reference.snaks[snakId].length > 0 ){
                                                                                    snaks[ snakId ] = reference.snaks[snakId]
                                                                                }
                                                                                return snaks
                                                                            } , {} )
                                                    }


                                                })


                                            }

                                            if( claim.qualifiers ){

                                                claim.qualifiers = Object.keys(claim.qualifiers)
                                                .filter( q => {
                                                    if( this.frProperties.includes(q) ){
                                                        return true
                                                    } else {
                                                        claim['qualifiers-order'].splice( claim['qualifiers-order'].indexOf(q) , 1 )
                                                        return false
                                                    }
                                                } )
                                                .reduce( (qualifiers,qualifierId) => {
                                                    qualifiers[qualifierId] = claim.qualifiers[qualifierId]
                                                    
                                                } , {} )

                                            }






                                        })


                                        return claims
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
