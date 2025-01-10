import random
import torch
import numpy as np

def dict_to_tensor(data_dict):
    # Initialize an empty list to hold numerical values
    values = []
    
    # Iterate through the dictionary
    for key, value in data_dict.items():
        # Check if the value is a number or nan
        if isinstance(value, (int, float)) and not np.isnan(value):
            values.append(value)
        else: 
            # Replace nan with 0 or any other placeholder
            values.append(0.0)  # You can change this to any other placeholder if needed
    
    # Convert the list of values to a PyTorch tensor
    tensor = torch.tensor(values, dtype=torch.float32)
    
    return tensor


class Buffer:
    """
    A FIFO buffer to store the weights of the vehicles.
    """
    def __init__(self, size, label=None):
        self.size = size
        self.buffer = []
        self.label = label


    def add(self, item):
        """
        Add an item to the buffer in the first position.
        """
        # add item to position 0:
        self.buffer.insert(0, item)
        if len(self.buffer) > self.size:
            # too much info in the buffer, remove the last item
            self.buffer.pop()


    def get(self):
        """
        This is a FIFO buffer, so we return the last item.
        """
        if len(self.buffer) > 0:
            return self.buffer[-1]
        else:
            return None
        
    def __len__(self):
        return len(self.buffer)