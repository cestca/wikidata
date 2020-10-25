const handleMessage = require( './common.js' ).handleMessage

module.exports = {
    handleMessage,

    start(){
        //this.isFirst = true
        this.kinds = []
        //console.log( '[' )
    },

    add( { kind } ){
        if( ! this.kinds.includes( kind ) ){
            this.kinds.push( kind )
        }
    },

    step( json , sendMessage ){

        let item = {}

        if( Object.keys( json.labels ).includes('fr') && Object.keys( json.descriptions ).includes('fr') ){

            item.id = json.id
            item.label = json.labels.fr.value
            item.description = json.descriptions.fr.value

            if( json.claims.P31 && json.claims.P31.length > 0 && json.claims.P31[0].mainsnak && json.claims.P31[0].mainsnak.datavalue && json.claims.P31[0].mainsnak.datavalue.value ){
                
                item.P31 = json.claims.P31[0].mainsnak.datavalue.value.id

                sendMessage( { method: 'add' , kind: item.P31 } )

                //console.log( ( this.isFirst ? '' : ', ' ) + JSON.stringify(item) )
                //this.isFirst = false
            }

        }


    },

    end(){
        console.log( JSON.stringify( this.kinds ) )
        //console.log( ']' )
    }


}