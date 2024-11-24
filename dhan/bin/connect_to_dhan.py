from dhanhq import dhanhq

class Connection:
    """Connection class will help in establishing connection to DhanHQ"""
    def __init__(self,client_id, access_token_id):
        self.__client_id = client_id
        self.__access_token_id = access_token_id
        pass

    @property
    def client_id(self):
        return self.__client_id
    
    @client_id.setter
    def client_id(self,value):
        if len(value)==10:
            try:
                clinetidint = int(value)
                self.__client_id = str(clinetidint)
            except Exception as e:
                raise ValueError(e)
        else:
            raise ValueError("Incorrect ClientID")
        
    @property
    def access_token_id(self):
        return self.__access_token_id
    
    @access_token_id.setter
    def access_token_id(self,value):
        if len(value)==280:
            self.__access_token_id = value
        else:
            raise ValueError("Access Token is not in right length")

    def connect_dhan(self):
        """ Establish Connection"""
        dhan = dhanhq(self.__client_id, self.__access_token_id)
        status = dhan.get_positions()
        if status['status']=='success':
            return({'status':'success',
                'conn':dhan,
                'client_id':self.__client_id,
                'access_id':self.__access_token_id})
        else:
            return({'status':'failure',
                'error':status['remarks']['error_type'],
                'client_id':self.__client_id,
                'access_id':self.__access_token_id})
