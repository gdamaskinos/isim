/*
 * Copyright (c) 2020 Georgios Damaskinos
 * All rights reserved.
 * @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

public class Rating {
	int userId;
	int itemId;
	double rating;

	public Rating(int userId, int itemId, double rating) {
		this.userId = userId;
		this.itemId = itemId;
		this.rating = rating;
	}

	public int getUserId() {
		return userId;
	}

	public int getItemId() {
		return itemId;
	}

	public double getRating() {
		return rating;
	}

}
