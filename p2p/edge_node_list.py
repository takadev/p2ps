import threading

class EdgeNodeList:
    def __init__(self):
        self.lock = threading.Lock()
        self.list = set()

    def add(self, edge):
        with self.lock:
            print('Adding edge to list: ', edge)
            self.list.add((edge))
            print('Current edge list: ', self.list)

    def remove(self, edge):
        with self.lock:
            if edge in self.list:
                print('Removing edge from list: ', edge)
                self.list.remove(edge)
                print('Current edge list: ', self.list)

    def overwrite(self, new_list):
        with self.lock:
            print('Edge node list will be going to override')
            self.list = new_list
            print('Current edge list: ', self.list)

    def get_list(self):
        return self.list