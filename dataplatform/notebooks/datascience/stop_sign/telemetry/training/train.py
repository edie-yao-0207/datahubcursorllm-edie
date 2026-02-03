# Databricks notebook source
import os

import numpy as np
from sklearn import metrics

import matplotlib.pyplot as plt
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
from torchvision import datasets, transforms, utils
import torchvision.models as models

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test & Training Dataframes

# COMMAND ----------

torch.cuda.empty_cache()

# COMMAND ----------

IMAGENET_MEANS = [0.485, 0.456, 0.406]
IMAGENET_STDDEV = [0.229, 0.224, 0.225]
img_transform = transforms.Compose(
    [
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean=IMAGENET_MEANS, std=IMAGENET_STDDEV),
    ]
)

# COMMAND ----------


class ImageFolderWithPaths(datasets.ImageFolder):
    """Custom dataset that includes image file paths. Extends
    torchvision.datasets.ImageFolder
    """

    # override the __getitem__ method. this is the method that dataloader calls
    def __getitem__(self, index):
        # this is what ImageFolder normally returns
        original_tuple = super(ImageFolderWithPaths, self).__getitem__(index)
        # the image file path
        path = self.imgs[index][0]
        # make a new tuple that includes original and the path
        tuple_with_path = original_tuple + (path,)
        return tuple_with_path


# COMMAND ----------

image_dir = (
    "/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/training_dataset/"
)
hist_data = ImageFolderWithPaths(image_dir, transform=img_transform)
print(len(hist_data))
TRAIN_LEN = int(0.8 * len(hist_data))
VAL_LEN = len(hist_data) - TRAIN_LEN
BATCH_SIZE = 64
NUM_CLASSES = 2
train_dataset, val_dataset = torch.utils.data.random_split(
    hist_data, [TRAIN_LEN, VAL_LEN]
)
trainloader = torch.utils.data.DataLoader(
    train_dataset, batch_size=BATCH_SIZE, num_workers=0, shuffle=True
)  # Higher numbers for num_workers can cause deadlock issues
valloader = torch.utils.data.DataLoader(
    val_dataset, batch_size=BATCH_SIZE, num_workers=0, shuffle=True
)
hist_data.class_to_idx

# COMMAND ----------

USE_CUDA = torch.cuda.is_available()

# COMMAND ----------

model = models.vgg19(pretrained=True)
# model.classifier[6] = nn.Linear(4096, NUM_CLASSES)
if USE_CUDA:
    model = model.cuda()

# COMMAND ----------

criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=0.001, amsgrad=True, weight_decay=0.001)
scheduler = optim.lr_scheduler.StepLR(optimizer, 10, gamma=0.1)

# COMMAND ----------

train_loss_arr = []
train_acc_arr = []
val_acc_arr = []
val_loss_arr = []
val_preds = []
val_probs = []
val_labels = []
MINI_BATCH_SIZE = 50
for epoch in range(50):  # loop over the dataset
    running_loss = 0.0
    num_correct = 0.0
    print(f"Starting Epoch {epoch + 1}")
    for i, data in enumerate(trainloader, 0):
        # get the inputs; data is a list of [inputs, labels]
        inputs, labels, _ = data
        if USE_CUDA:
            inputs, labels = inputs.cuda(), labels.cuda()

        # zero the parameter gradients
        optimizer.zero_grad()

        # forward + backward + optimize
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        # print statistics
        running_loss += float(loss.item())
        _, predicted = torch.max(outputs, 1)
        num_correct += (predicted == labels).sum().item()
        if i % MINI_BATCH_SIZE == MINI_BATCH_SIZE - 1:  # print every 100 mini-batches

            train_loss_arr.append(running_loss / MINI_BATCH_SIZE)
            train_acc = float(num_correct / (BATCH_SIZE * MINI_BATCH_SIZE))
            train_acc_arr.append(train_acc)
            print(
                "[%d, %5d] training loss: %.9f training accuracy: %.9f"
                % (epoch + 1, i + 1, running_loss / MINI_BATCH_SIZE, train_acc)
            )
            running_loss = 0.0
            num_correct = 0

            # validation accuracy
            model.eval()
            with torch.no_grad():
                val_loss = 0.0
                val_correct = 0
                batch_num = 0
                for i, data in enumerate(valloader):
                    batch_num += 1
                    inputs, labels, _ = data
                    if USE_CUDA:
                        inputs, labels = inputs.cuda(), labels.cuda()
                    outputs = model(inputs)
                    _, predicted = torch.max(outputs, 1)
                    loss = criterion(outputs, labels)
                    val_loss += float(loss.item())
                    val_correct += (predicted == labels).sum().item()

                    # get probabilities
                    sm = torch.nn.Softmax()
                    probabilities = sm(outputs)
                    # probability of stop sign class
                    val_preds.extend(
                        [
                            (1 - float(x[0]))
                            for x in probabilities.detach().cpu().numpy()
                        ]
                    )
                    val_labels.extend([float(x) for x in labels.detach().cpu().numpy()])
            val_loss_arr.append(val_loss / batch_num)
            val_acc = float(val_correct / (batch_num * BATCH_SIZE))
            val_acc_arr.append(val_acc)
            print(
                "[%d, %5d] validation loss: %.9f validation accuracy: %.9f"
                % (epoch + 1, i + 1, val_loss / batch_num, val_acc)
            )
            model.train()
    scheduler.step()
print("Finished Training")

# COMMAND ----------

inputs, _, paths = next(iter(valloader))
if USE_CUDA:
    inputs = inputs.cuda()
outputs = model(inputs)
probs, predicted = torch.max(outputs, 1)
class_predictions = ["non_stop_sign" if p == 0 else "stop_sign" for p in predicted]

# COMMAND ----------

plt.imshow(np.transpose(utils.make_grid(inputs).cpu().numpy(), (1, 2, 0)))
plt.show()
display()

# COMMAND ----------

print(predicted)

# COMMAND ----------

for path, prediction in zip(paths, class_predictions):
    print(f"Predicted: {prediction} Path: {path}")

# COMMAND ----------

# Plot Accuracy
plt.plot(np.linspace(0, 15, len(train_acc_arr)), train_acc_arr)
plt.plot(np.linspace(0, 15, len(val_acc_arr)), val_acc_arr)
plt.legend(["Training Accuracy", "Validation Accuracy"])
plt.title("Accuracy")
plt.xlabel("Epoch")
plt.ylabel("Accuracy")
display()

# COMMAND ----------

# Plot Loss
plt.plot(np.linspace(0, 15, len(train_loss_arr)), train_loss_arr)
plt.plot(np.linspace(0, 15, len(val_loss_arr)), val_loss_arr)
plt.legend(["Training Loss", "Validation Loss"])
plt.title("Loss")
plt.xlabel("Epoch")
plt.ylabel("Loss")
display()

# COMMAND ----------

# Plot ROC curve
fpr, tpr, _ = metrics.roc_curve(val_labels, val_preds)
plt.plot(fpr, tpr)
plt.title("ROC Curve")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
display()

# COMMAND ----------

PATH = "/dbfs/mnt/samsara-databricks-playground-bucket/stop_sign_detection/stop_sign_combined_dataset_pytorch_model.pth"
torch.save(model.state_dict(), PATH)

# COMMAND ----------
