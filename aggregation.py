import numpy as np

def federated_averaging(global_model, participant_models):
    num_participants = len(participant_models)
    if num_participants == 0:
        raise ValueError("Non ci sono modelli partecipanti per l'averaging.")

    # Estrai i pesi dal primo modello per inizializzare la struttura della somma
    sum_weights = [np.zeros_like(weights) for weights in global_model.get_weights()]

    # Somma i pesi di tutti i modelli partecipanti
    for participant_model in participant_models:
        participant_weights = participant_model.get_weights()
        for i, layer_weights in enumerate(participant_weights):
            sum_weights[i] += layer_weights

    # Calcola la media dei pesi
    averaged_weights = [sum_weight / num_participants for sum_weight in sum_weights]

    # Verifica che averaged_weights sia una lista di array NumPy
    if not isinstance(averaged_weights, list) or not all(isinstance(w, np.ndarray) for w in averaged_weights):
        raise TypeError("averaged_weights deve essere una lista di array NumPy.")

    return averaged_weights


def fed_yogi(global_model_params, participant_models, learning_rate=0.001, beta1=0.9, beta2=0.999, epsilon=1e-3):
    moment_1 = None
    moment_2 = None
    step = 0

    num_participants = len(participant_models)
    if num_participants == 0:
        raise ValueError("Non ci sono modelli partecipanti per l'aggregazione.")

    sum_gradients = [np.zeros_like(weights) for weights in global_model_params]

    for participant_model in participant_models:
        participant_weights = participant_model.get_weights()  # Assicurati che sia un modello e non un array
        global_weights = global_model_params
        gradients = [participant_weight - global_weight for participant_weight, global_weight in zip(participant_weights, global_weights)]
        for i, layer_grad in enumerate(gradients):
            sum_gradients[i] += layer_grad

    averaged_gradients = [sum_grad / num_participants for sum_grad in sum_gradients]

    if moment_1 is None:
        moment_1 = [np.zeros_like(weights) for weights in global_model_params]
    if moment_2 is None:
        moment_2 = [np.zeros_like(weights) for weights in global_model_params]

    for i, grad in enumerate(averaged_gradients):
        step += 1
        moment_1[i] = beta1 * moment_1[i] + (1 - beta1) * grad
        moment_2[i] += (1 - beta2) * (grad ** 2 - moment_2[i])

        m_hat = moment_1[i] / (1 - beta1 ** step)
        v_hat = moment_2[i] / (1 - beta2 ** step)

        global_model_params[i] -= learning_rate * m_hat / (np.sqrt(v_hat) + epsilon)

    return global_model_params