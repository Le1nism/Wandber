import numpy as np


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


def fed_yogi(global_model_state_dict, participant_models, learning_rate=0.001, beta1=0.9, beta2=0.999, epsilon=1e-3):
    moment_1 = None
    moment_2 = None
    step = 0

    num_participants = len(participant_models)
    if num_participants == 0:
        raise ValueError("No participant models for averaging.")

    sum_gradients = [np.zeros_like(weights) for weights in global_model_state_dict]

    for participant_model in participant_models:
        participant_weights = participant_model.get_weights()  # Assicurati che sia un modello e non un array
        global_weights = global_model_state_dict
        gradients = [participant_weight - global_weight for participant_weight, global_weight in zip(participant_weights, global_weights)]
        for i, layer_grad in enumerate(gradients):
            sum_gradients[i] += layer_grad

    averaged_gradients = [sum_grad / num_participants for sum_grad in sum_gradients]

    if moment_1 is None:
        moment_1 = [np.zeros_like(weights) for weights in global_model_state_dict]
    if moment_2 is None:
        moment_2 = [np.zeros_like(weights) for weights in global_model_state_dict]

    for i, grad in enumerate(averaged_gradients):
        step += 1
        moment_1[i] = beta1 * moment_1[i] + (1 - beta1) * grad
        moment_2[i] += (1 - beta2) * (grad ** 2 - moment_2[i])

        m_hat = moment_1[i] / (1 - beta1 ** step)
        v_hat = moment_2[i] / (1 - beta2 ** step)

        global_model_state_dict[i] -= learning_rate * m_hat / (np.sqrt(v_hat) + epsilon)

    return global_model_state_dict