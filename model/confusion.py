from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import numpy as np

def plot_confusion_matrix(cm, title='Confusion Matrix', cmap=plt.cm.Blues):
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(2)
    plt.xticks(tick_marks, [0, 1])
    plt.yticks(tick_marks, [0, 1])
    plt.tight_layout()
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')

def main():
    '''plot the confusion matrix from the data'''
    #compute confusion matrix
    # read file of y_pred, y_true
    y_true, y_pred = *([], [])
    cm_logistic = confusion_matrix(y_true, y_pred)
    np.set_printoptions(precision=2)
    print 'Confusion Matrix'
    print cm_logistic
    plt.figure()
    plot_confusion_matrix(cm_logistic)

if __name__ == "__main__":
    filename = sys.argv[1]
    main(sc, filename)
