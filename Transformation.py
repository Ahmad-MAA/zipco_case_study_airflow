import pandas as pd
def run_transformation():
    data = pd.read_csv(r'zipco_transaction.csv')

    # Data cleaning and transformation
    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handle missing values( filling missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include =['float', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace =True)

    # Handling missing values (filling missing string object values with 'unknown')
    String_columns = data.select_dtypes(include =['object']).columns
    for col in String_columns:
        data.fillna({col: 'Unknown'}, inplace=True)


    # Assigning the right data type
    data['Date'] = pd.to_datetime(data['Date'])


    #Create products table
    products= data [['ProductName' ]].copy().drop_duplicates().reset_index(drop=True)
    products.index.name ='ProductID'
    products = products.reset_index()


    #create customers table
    customers =data [['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail']].copy().drop_duplicates().reset_index(drop=True)
    customers.index.name ='CustomerID'
    customers = customers.reset_index()


    # creating the staff
    staff =data [['Staff_Name','Staff_Email']].copy().drop_duplicates().reset_index(drop=True)
    staff.index.name ='StaffID'
    staff = staff.reset_index()


    # Creating the transactons tabel
    transaction = data.merge(products, on=['ProductName'], how = 'left') \
                        .merge(customers, on = ['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail'], how ='left' ) \
                        .merge(staff, on= ['Staff_Name','Staff_Email'], how ='left' )
    transaction.index.name ='TransactionID'
    transaction = transaction.reset_index()\
                                [['Date', 'ProductName', 'Quantity', 'UnitPrice', 'StoreLocation','ProductID', \
                                'PaymentType', 'PromotionApplied', 'Weather', 'Temperature','StaffPerformanceRating','CustomerID', \
                                    'CustomerFeedback', 'DeliveryTime_min','OrderType','StaffID','DayOfWeek','TotalSales']]



    # Save data to as csv files
    data.to_csv('clean_data.csv', index=False)
    products.to_csv('products.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)


    print('Data cleaning and Transformation Completed')