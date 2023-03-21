import clip
import logging
import numpy as np
import torch
from tqdm import tqdm
from io import BytesIO
from PIL import Image


class OutfitDataset(torch.utils.data.Dataset):
    def __init__(self, img_blobs, captions, device):
        _, preprocess_fn = clip.load("ViT-B/32", device=device)

        self.img_blobs = img_blobs
        self.preprocessed_images = [
            preprocess_fn(Image.open(BytesIO(content)))
            for content in img_blobs
        ]
        self.captions = captions
        print("Done preprocessing images.")

    def __getitem__(self, index):
        preprocessed_image = self.preprocessed_images[index]
        caption = self.captions[index]
        return preprocessed_image, caption

    def __len__(self):
        return len(self.captions)


def convert_models_to_fp32(model):
    for p in model.parameters():
        p.data = p.data.float()
        p.grad.data = p.grad.data.float()


def filter_list(elements, positions):
    return [elements[position] for position in positions]


def fine_tune_model(model, img_blobs, captions, batch_size=16, epochs=5):
    device = "cuda" if torch.cuda.is_available() else "cpu"

    # Get ids for a train split
    perm = np.random.permutation(len(captions))
    train_ids = perm[: int(0.8 * len(captions))]
    test_ids = perm[int(0.8 * len(captions)) :]

    train_dataset = OutfitDataset(
        filter_list(img_blobs, train_ids),
        filter_list(captions, train_ids),
        device,
    )
    print(f"Train dataset size: {len(train_dataset)}")
    test_dataset = OutfitDataset(
        filter_list(img_blobs, test_ids),
        filter_list(captions, test_ids),
        device,
    )
    print(f"Test dataset size: {len(test_dataset)}")
    if device == "cpu":
        model.float()

    # Create dataloader
    train_dataloader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size, shuffle=True
    )
    test_dataloader = torch.utils.data.DataLoader(
        test_dataset, batch_size=batch_size, shuffle=False
    )

    # Initialize loss and optimizer
    loss_img = torch.nn.CrossEntropyLoss()
    loss_txt = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-5)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, len(train_dataloader) * epochs
    )
    best_loss = float("inf")

    # Train and validate model
    for epoch in range(epochs):
        # Train mode
        model.train()
        pbar = tqdm(train_dataloader, leave=False)
        step = 0
        train_loss = 0
        for images, captions in pbar:
            step += 1
            images = images.to(device)
            captions = clip.tokenize(captions).to(device)
            num_images = images.shape[0]

            optimizer.zero_grad()

            image_logits, text_logits = model(images, captions)

            loss = loss_img(
                image_logits, torch.arange(num_images).to(device)
            ) + loss_txt(text_logits, torch.arange(num_images).to(device))
            loss.backward()
            train_loss += loss.item()

            if device != "cpu":
                convert_models_to_fp32(model)

            optimizer.step()
            scheduler.step()

            if device != "cpu":
                clip.model.convert_weights(model)

            pbar.set_description(
                f"Train batch loss: {loss.item()}", refresh=True
            )
        train_loss /= step

        # Validation mode
        with torch.no_grad():
            model.eval()
            pbar = tqdm(test_dataloader, leave=False)
            step = 0
            eval_loss = 0

            for images, captions in pbar:
                step += 1
                images = images.to(device)
                captions = clip.tokenize(captions).to(device)
                num_images = images.shape[0]

                image_logits, text_logits = model(images, captions)

                loss = loss_img(
                    image_logits, torch.arange(num_images).to(device)
                ) + loss_txt(text_logits, torch.arange(num_images).to(device))
                eval_loss += loss.item()

                pbar.set_description(
                    f"Eval batch loss: {loss.item()}", refresh=True
                )

        eval_loss /= step
        print(f"Train epoch {epoch} loss: {train_loss}")
        print(f"Eval epoch {epoch} loss: {eval_loss}")
        if eval_loss > best_loss:
            break

        best_loss = eval_loss

    # Return model
    return model
