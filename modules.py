import torch.nn as nn
import torch.nn.init as init

class MLP(nn.Module):
    def __init__(self, input_dim, output_dim, **kwargs):
        super(MLP, self).__init__()
        h_dim = kwargs.get('h_dim', 128)
        dropout = kwargs.get('dropout', 0.1)
        num_layers = kwargs.get('num_layers', 3)

        layers = []
        curr_output_dim = h_dim
        curr_input_dim = input_dim
        for _ in range(num_layers):
            layers.append(nn.Linear(curr_input_dim, curr_output_dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout))
            curr_input_dim = curr_output_dim
            curr_output_dim = curr_output_dim // 2

        layers.append(nn.Linear(curr_input_dim, output_dim))
        layers.append(nn.Sigmoid())

        self.model = nn.Sequential(*layers)

    def forward(self, x):
        return self.model(x)
    
    def initialize_weights(self, strategy='xavier'):
        for m in self.modules():
            if isinstance(m, nn.Linear):
                if strategy == 'xavier':
                    # Xavier Initialization
                    init.xavier_uniform_(m.weight)
                    if m.bias is not None:
                        init.zeros_(m.bias)
                elif strategy == 'he':
                    # He Initialization
                    init.kaiming_normal_(m.weight)
                    if m.bias is not None:
                        init.zeros_(m.bias)
                elif strategy == 'normal':
                    # Normal Initialization
                    init.normal_(m.weight, 0, 0.01)
                    if m.bias is not None:
                        init.zeros_(m.bias)
                else:
                    raise ValueError(f"Unknown local initialization strategy: {strategy}")
                
