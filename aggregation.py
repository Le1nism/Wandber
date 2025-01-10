import numpy as np
import torch

def federated_averaging(global_model_state_dict, participant_models_state_dict):
    """
    Aggregates the state dict from all participant models using Federated Averaging.
    """
    num_participants = len(participant_models_state_dict)
    if num_participants == 0:
        raise ValueError("No participant models for averaging.")
    # Aggregate!
    averaged_state_dict = {}
    for key in global_model_state_dict.keys():
        averaged_state_dict[key] = sum([participant_model_state_dict[key] for participant_model_state_dict in participant_models_state_dict]) / num_participants
    return averaged_state_dict


def fed_yogi(global_model_state_dict, participant_models_state_dicts, **kwargs):

    learning_rate = kwargs.get('learning_rate', 0.001)
    beta1 = kwargs.get('yogi_beta1', 0.9)
    beta2 = kwargs.get('yogi_beta2', 0.999)
    epsilon = kwargs.get('yogi_epsilon', 1e-3)


    num_participants = len(participant_models_state_dicts)
    if num_participants == 0:
        raise ValueError("No participant models for averaging.")

    sum_gradients = {}
    for key, value in global_model_state_dict.items():
        sum_gradients[key] = torch.zeros_like(value)


    for participant_model_state_dict in participant_models_state_dicts:
        for key, value in participant_model_state_dict.items():
            sum_gradients[key] += (value - global_model_state_dict[key]) / num_participants

    
    moment_1 = {key: torch.zeros_like(value) for key, value in global_model_state_dict.items()}
    moment_2 = {key: torch.zeros_like(value) for key, value in global_model_state_dict.items()}

    for step, (key, grad) in enumerate(sum_gradients.items()):
        
        moment_1[key] = beta1 * moment_1[key] + (1 - beta1) * grad
        moment_2[key] += (1 - beta2) * (grad ** 2 - moment_2[key])

        m_hat = moment_1[key] / (1 - beta1 ** step)
        v_hat = moment_2[key] / (1 - beta2 ** step)

        global_model_state_dict[key] -= learning_rate * m_hat / (torch.sqrt(v_hat) + epsilon)

    return global_model_state_dict