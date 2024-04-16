// src/index.js

const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');

function performInnerJoin(mainData, joinData, joinCondition, selectedFields, mainTable) {
    return mainData.flatMap((mainRow) => {
      return joinData
        .filter((joinRow) => {
          const mainValue = mainRow[joinCondition.left.split(".")[1]];
          const joinValue = joinRow[joinCondition.right.split(".")[1]];
          return mainValue === joinValue;
        })
        .map((joinRow) => {
          return selectedFields.reduce((acc, field) => {
            const [tableName, fieldName] = field.split(".");
            acc[field] =
              tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
            return acc;
          }, {});
        });
    });
  }
  
  
  function performLeftJoin(mainData, joinData, joinCondition, selectedFields, mainTable) {
    return mainData.flatMap((mainRow) => {
      const matchedJoinRows = joinData.filter((joinRow) => {
        const mainValue = getValueFromRow(mainRow, joinCondition.left);
        const joinValue = getValueFromRow(joinRow, joinCondition.right);
        return mainValue === joinValue;
      });
  
      if (matchedJoinRows.length === 0) {
        return [createResultRow(mainRow, null, selectedFields, mainTable, true)];
      }
  
      return matchedJoinRows.map((joinRow) =>
        createResultRow(mainRow, joinRow, selectedFields, mainTable, true)
      );
    });
  }
  
  
  function getValueFromRow(row, compoundFieldName) {
    const [tableName, fieldName] = compoundFieldName.split(".");
    return row[`${tableName}.${fieldName}`] || row[fieldName];
  }
  function performRightJoin(mainData, joinData, joinCondition, selectedFields, mainTable) {
    const mainTableStructure =
      mainData.length > 0
        ? Object.keys(mainData[0]).reduce((acc, key) => {
            acc[key] = null; // Initialize values to null
            return acc;
          }, {})
        : {};
  
    return joinData.map((joinRow) => {
      const matchingMainRow = mainData.find((mainRow) => {
        const mainValue = getValueFromRow(mainRow, joinCondition.left);
        const joinValue = getValueFromRow(joinRow, joinCondition.right);
        return mainValue === joinValue;
      });
  
      const mainRowToUse = matchingMainRow || mainTableStructure;
  
      return createResultRow(mainRowToUse, joinRow, selectedFields, mainTable, true);
    });
  }
  
  function createResultRow(mainRow, joinRow, selectedFields, mainTable, includeAllMainFields) {
    const resultRow = {};
  
    if (includeAllMainFields) {
      Object.keys(mainRow || {}).forEach((key) => {
        const prefixedKey = `${mainTable}.${key}`;
        resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
      });
    }
  
    selectedFields.forEach((field) => {
      const [tableName, fieldName] = field.includes(".")
        ? field.split(".")
        : [mainTable, field];
      resultRow[field] =
        tableName === mainTable && mainRow
          ? mainRow[fieldName]
          : joinRow
          ? joinRow[fieldName]
          : null;
    });
  
    return resultRow;
  }

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition, joinType } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Perform INNER JOIN if specified
    // Logic for applying JOINs
    
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Handle default case or unsupported JOIN types
            default:
                throw new Error(`Unsupported JOIN type: ${joinType}`);
        }
    }

    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}



function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;