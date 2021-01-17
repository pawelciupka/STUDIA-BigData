import time

class TimeCalculation:
    time = None
    
    def start_time(self, method_name):
        self.time = time.time()
        print(method_name + " - BEG") 

    def end_time(self, method_name):        
        elapsed_time = (time.time() - self.time)
        print(method_name + " - END in " + str(elapsed_time) + " s.")
        self.time = None