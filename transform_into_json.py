# %%
import mysql.connector
import json
import os
# Current working directory
cwd = os.getcwd()
# Database connection parameters
def main():    
    db_config = {
        'user': 'HideaJ',
        'password': '',
        'host': 'localhost',
        'database': 'newschema'
    }
    
    # Establishing a database connection
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'AddressID', AddressID, 
        'AddressLine1', AddressLine1, 
        'AddressLine2', AddressLine2, 
        'City', City, 
        'StateProvinceID', StateProvinceID, 
        'PostalCode', PostalCode, 
        'SpatialLocation', SpatialLocation, 
        'rowguid', rowguid, 
        'ModifiedDate', ModifiedDate
    ) AS json_data 
    FROM person_address;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' + 'person_address.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'DepartmentID', DepartmentID, 
        'Name', Name, 
        'GroupName', GroupName, 
        'ModifiedDate', ModifiedDate
    ) AS json_data
    FROM humanresources_department;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'humanresources_department.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'NationalIDNumber', NationalIDNumber, 
        'LoginID', LoginID, 
        'OrganizationNode', OrganizationNode, 
        'OrganizationLevel', OrganizationLevel, 
        'JobTitle', JobTitle, 
        'BirthDate', BirthDate, 
        'MaritalStatus', MaritalStatus, 
        'Gender', Gender,
        'HireDate',HireDate,
        'SalariedFlag',SalariedFlag,
        'VacationHours',VacationHours,
        'SickLeaveHours',SickLeaveHours,
        'CurrentFlag',CurrentFlag,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM humanresources_employee;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'humanresources_employee.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'RateChangeDate', RateChangeDate, 
        'Rate', Rate, 
        'PayFrequency', PayFrequency, 
        'ModifiedDate', ModifiedDate
    ) AS json_data 
    FROM humanresources_employeepayhistory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'humanresources_employeepayhistory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # %%
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT( 
        'BusinessEntityID', BusinessEntityID, 
        'DepartmentID', DepartmentID, 
        'ShiftID', ShiftID, 
        'StartDate', StartDate, 
        'EndDate', EndDate, 
        'ModifiedDate', ModifiedDate
    ) AS json_data 
    FROM humanresources_employeedepartmenthistory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'humanresources_employeedepartmenthistory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # %%
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ShiftID', ShiftID, 
        'Name', Name, 
        'StartTime', StartTime, 
        'EndTime', EndTime, 
        'ModifiedDate', ModifiedDate
    ) AS json_data 
    FROM humanresources_shift;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'humanresources_shift.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'AddressTypeID', AddressTypeID, 
        'Name', Name, 
        'rowguid', rowguid, 
        'ModifiedDate', ModifiedDate
    ) AS json_data 
    FROM person_addresstype;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_addresstype.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_businessentity;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_businessentity.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'AddressID', AddressID, 
        'AddressTypeID', AddressTypeID,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_businessentityaddress;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_businessentityaddress.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'PersonID', PersonID, 
        'ContactTypeID', ContactTypeID,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_businessentitycontact;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_businessentitycontact.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ContactTypeID', ContactTypeID, 
        'Name', Name, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_contacttype;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_contacttype.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'CountryRegionCode', CountryRegionCode, 
        'Name', Name, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_countryregion;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_countryregion.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'EmailAddressID', EmailAddressID, 
        'EmailAddress', EmailAddress, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_emailaddress;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_emailaddress.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'PersonType', PersonType, 
        'NameStyle', NameStyle, 
        'Title', Title, 
        'FirstName', FirstName, 
        'MiddleName', MiddleName, 
        'LastName', LastName, 
        'Suffix', Suffix, 
        'EmailPromotion', EmailPromotion,
        'AdditionalContactInfo',AdditionalContactInfo,
        'Demographics',Demographics,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_person;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_person.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'StateProvinceID', StateProvinceID, 
        'StateProvinceCode', StateProvinceCode, 
        'CountryRegionCode', CountryRegionCode, 
        'IsOnlyStateProvinceFlag', IsOnlyStateProvinceFlag, 
        'Name', Name, 
        'TerritoryID', TerritoryID, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM person_stateprovince;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'person_stateprovince.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BillOfMaterialsID', BillOfMaterialsID, 
        'ProductAssemblyID', ProductAssemblyID, 
        'ComponentID', ComponentID, 
        'StartDate', StartDate, 
        'EndDate', EndDate, 
        'UnitMeasureCode', UnitMeasureCode, 
        'BOMLevel', BOMLevel, 
        'PerAssemblyQty', PerAssemblyQty,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_billofmaterials;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_billofmaterials.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # %%
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'CultureID', CultureID, 
        'Name', Name, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_culture;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_culture.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'LocationID', LocationID, 
        'Name', Name, 
        'CostRate', CostRate, 
        'Availability', Availability, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_location;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_location.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductID', ProductID, 
        'Name', Name, 
        'ProductNumber', ProductNumber, 
        'MakeFlag', MakeFlag, 
        'FinishedGoodsFlag', FinishedGoodsFlag, 
        'Color', Color, 
        'SafetyStockLevel', SafetyStockLevel, 
        'ReorderPoint', ReorderPoint, 
        'StandardCost', StandardCost,
        'ListPrice',ListPrice,
        'Size',Size,
        'SizeUnitMeasureCode',SizeUnitMeasureCode,
        'WeightUnitMeasureCode',WeightUnitMeasureCode,
        'Weight',Weight,
        'DaysToManufacture', DaysToManufacture,
        'ProductLine', ProductLine,
        'Class', Class,
        'Style', Style,
        'ProductSubcategoryID', ProductSubcategoryID,
        'ProductModelID', ProductModelID,
        'SellStartDate', SellStartDate,
        'SellEndDate', SellEndDate,
        'DiscontinuedDate', DiscontinuedDate,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_product;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_product.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductCategoryID', ProductCategoryID, 
        'Name', Name, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_productcategory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productcategory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductDescriptionID', ProductDescriptionID, 
        'Description', Description, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_productdescription;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productdescription.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductID', ProductID, 
        'LocationID', LocationID, 
        'Shelf', Shelf, 
        'Bin', Bin, 
        'Quantity', Quantity, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_productinventory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productinventory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductID', ProductID, 
        'StartDate', StartDate, 
        'EndDate', EndDate, 
        'ListPrice', ListPrice, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_productlistpricehistory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productlistpricehistory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductModelID', ProductModelID, 
        'ProductDescriptionID', ProductDescriptionID, 
        'CultureID', CultureID, 
        'ModifiedDate', ModifiedDate
    ) AS json_data 
    FROM production_productmodelproductdescriptionculture;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productmodelproductdescriptionculture.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductSubcategoryID', ProductSubcategoryID, 
        'ProductCategoryID', ProductCategoryID, 
        'Name', Name, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_productsubcategory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productsubcategory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'TransactionID', TransactionID, 
        'ProductID', ProductID, 
        'ReferenceOrderID', ReferenceOrderID, 
        'ReferenceOrderLineID', ReferenceOrderLineID, 
        'TransactionDate', TransactionDate, 
        'TransactionType', TransactionType, 
        'Quantity', Quantity, 
        'ActualCost', ActualCost, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_transactionhistory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' + 'production_transactionhistory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'WorkOrderID', WorkOrderID, 
        'ProductID', ProductID, 
        'OrderQty', OrderQty, 
        'StockedQty', StockedQty, 
        'ScrappedQty', ScrappedQty, 
        'StartDate', StartDate, 
        'EndDate', EndDate, 
        'DueDate', DueDate, 
        'ScrapReasonID', ScrapReasonID,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_workorder;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_workorder.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductID', ProductID, 
        'BusinessEntityID', BusinessEntityID, 
        'AverageLeadTime', AverageLeadTime, 
        'StandardPrice', StandardPrice, 
        'LastReceiptCost', LastReceiptCost, 
        'LastReceiptDate', LastReceiptDate, 
        'MinOrderQty', MinOrderQty, 
        'MaxOrderQty', MaxOrderQty, 
        'OnOrderQty', OnOrderQty,
        'UnitMeasureCode',UnitMeasureCode,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM purchasing_productvendor;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'purchasing_productvendor.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'PurchaseOrderID', PurchaseOrderID, 
        'PurchaseOrderDetailID', PurchaseOrderDetailID, 
        'DueDate', DueDate, 
        'OrderQty', OrderQty, 
        'ProductID', ProductID, 
        'UnitPrice', UnitPrice, 
        'LineTotal', LineTotal, 
        'ReceivedQty', ReceivedQty, 
        'RejectedQty', RejectedQty,
        'StockedQty',StockedQty,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM purchasing_purchaseorderdetail;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'purchasing_purchaseorderdetail.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'PurchaseOrderID', PurchaseOrderID, 
        'RevisionNumber', RevisionNumber, 
        'Status', Status, 
        'EmployeeID', EmployeeID, 
        'VendorID', VendorID, 
        'ShipMethodID', ShipMethodID, 
        'OrderDate', OrderDate, 
        'ShipDate', ShipDate, 
        'SubTotal', SubTotal,
        'TaxAmt',TaxAmt,
        'Freight',Freight,
        'TotalDue',TotalDue,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM purchasing_purchaseorderheader;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'purchasing_purchaseorderheader.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ShipMethodID', ShipMethodID, 
        'Name', Name, 
        'ShipBase', ShipBase, 
        'ShipRate', ShipRate, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM purchasing_shipmethod;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'purchasing_shipmethod.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'AccountNumber', AccountNumber, 
        'Name', Name, 
        'CreditRating', CreditRating, 
        'PreferredVendorStatus', PreferredVendorStatus, 
        'ActiveFlag', ActiveFlag, 
        'PurchasingWebServiceURL', PurchasingWebServiceURL, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM purchasing_vendor;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'purchasing_vendor.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'CountryRegionCode', CountryRegionCode, 
        'CurrencyCode', CurrencyCode, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_countryregioncurrency;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_countryregioncurrency.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'CreditCardID', CreditCardID, 
        'CardType', CardType, 
        'CardNumber', CardNumber, 
        'ExpMonth', ExpMonth, 
        'ExpYear', ExpYear, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_creditcard;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_creditcard.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'CustomerID', CustomerID, 
        'PersonID', PersonID, 
        'StoreID', StoreID, 
        'TerritoryID', TerritoryID, 
        'AccountNumber', AccountNumber, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_customer;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_customer.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'CreditCardID', CreditCardID, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_personcreditcard;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_personcreditcard.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'SalesOrderID', SalesOrderID, 
        'SalesOrderDetailID', SalesOrderDetailID, 
        'CarrierTrackingNumber', CarrierTrackingNumber, 
        'OrderQty', OrderQty, 
        'ProductID', ProductID, 
        'SpecialOfferID', SpecialOfferID, 
        'UnitPrice', UnitPrice, 
        'UnitPriceDiscount', UnitPriceDiscount, 
        'LineTotal', LineTotal,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_salesorderdetail;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_salesorderdetail.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'SalesReasonID', SalesReasonID, 
        'Name', Name, 
        'ReasonType', ReasonType, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_salesreason;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_salesreason.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'SpecialOfferID', SpecialOfferID, 
        'ProductID', ProductID, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_specialofferproduct;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_specialofferproduct.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'SalesOrderID', SalesOrderID, 
        'RevisionNumber', RevisionNumber, 
        'OrderDate', OrderDate, 
        'DueDate', DueDate, 
        'ShipDate', ShipDate, 
        'Status', Status, 
        'OnlineOrderFlag', OnlineOrderFlag, 
        'SalesOrderNumber', SalesOrderNumber, 
        'PurchaseOrderNumber', PurchaseOrderNumber,
        'AccountNumber',AccountNumber,
        'CustomerID',CustomerID,
        'SalesPersonID',SalesPersonID,
        'TerritoryID',TerritoryID,
        'BillToAddressID',BillToAddressID,
        'ShipToAddressID', ShipToAddressID,
        'ShipMethodID', ShipMethodID,
        'CreditCardID', CreditCardID,
        'CreditCardApprovalCode', CreditCardApprovalCode,
        'CurrencyRateID', CurrencyRateID,
        'SubTotal', SubTotal,
        'TaxAmt', TaxAmt,
        'Freight', Freight,
        'TotalDue', TotalDue,
        'Comment', Comment,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_salesorderheader;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_salesorderheader.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'TerritoryID', TerritoryID, 
        'Name', Name, 
        'CountryRegionCode', CountryRegionCode, 
        'Group', `Group`, 
        'SalesYTD', SalesYTD, 
        'SalesLastYear', SalesLastYear, 
        'CostYTD', CostYTD, 
        'CostLastYear', CostLastYear, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_salesterritory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_salesterritory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'SpecialOfferID', SpecialOfferID, 
        'Description', Description, 
        'DiscountPct', DiscountPct, 
        'Type', Type, 
        'Category', Category, 
        'StartDate', StartDate, 
        'EndDate', EndDate, 
        'MinQty', MinQty, 
        'MaxQty', MaxQty,
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_specialoffer;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_specialoffer.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'BusinessEntityID', BusinessEntityID, 
        'Name', Name, 
        'SalesPersonID', SalesPersonID, 
        'Demographics', Demographics, 
        'rowguid',rowguid,
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM sales_store;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'sales_store.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    
    
    # SQL Query
    query = """
    SELECT JSON_OBJECT(
        'ProductID', ProductID, 
        'StartDate', StartDate, 
        'EndDate', EndDate, 
        'StandardCost', StandardCost, 
        'ModifiedDate',ModifiedDate
    ) AS json_data 
    FROM production_productcosthistory;
    """
    
    cursor.execute(query)
    
    # Fetch all rows and convert to a list of JSON objects
    rows = [json.loads(row[0]) for row in cursor.fetchall()]
    
    # Writing data to a JSON file
    with open(cwd + '\\json_files\\' +'production_productcosthistory.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)
    
    # Closing the connection
    cursor.close()
    conn.close()

    return 1
    
