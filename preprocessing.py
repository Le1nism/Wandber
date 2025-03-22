import random
import torch


class GenericBuffer:
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
        
    def pop(self):
        """
        Remove the last item from the buffer.
        """
        if len(self.buffer) > 0:
            self.buffer.pop()


    def __len__(self):
        return len(self.buffer)


class HealthProbesBuffer:
    def __init__(self, size, label=None):
        self.size = size
        self.buffer = []
        self.label = label

    def add(self, item):
        self.buffer.append(item)
        if len(self.buffer) > self.size:
            self.buffer.pop(0)


    def sample(self, n):
        if len(self.buffer) < n:
            record_list = self.buffer
        else:
            record_list = random.sample(self.buffer, n)

        feature_tensors = []
        class_labels = []

        feature_tensors = torch.tensor(
            [list(record.values()) for record in record_list],
            dtype=torch.float32
        )

        feature_tensors = torch.nan_to_num(feature_tensors, nan=0.0, posinf=1e6, neginf=-1e6)

        if len(record_list) > 0:
            class_labels = torch.tensor([[self.label]] * feature_tensors.shape[0]).to(torch.float32)

        
        return feature_tensors, class_labels