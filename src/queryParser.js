// src/queryParser.js

function parseQuery(query) {
    const selectRegex = /SELECT (.+?) FROM (.+?)(?: WHERE (.*))?$/i;
    const match = query.match(selectRegex);

    if (match) {
        const [, fields, table, whereString] = match;
        const whereClauses = whereString ? parseWhereClause(whereString) : [];
        return {
            fields: fields.split(',').map(field => field.trim()),
            table: table.trim(),
            whereClauses
        };
    } else {
        throw new Error('Invalid query format');
    }
}

function parseWhereClause(whereString) {
    const conditions = whereString.split(/ AND | OR /i);
    if(conditions){
        return conditions.map(condition => {
            const [field, operator, value] = condition.split(/\s+/);
            return { field, operator, value };
        });
    } else {
        throw new Error('Invalid query format');
    }
    
}

module.exports = parseQuery;