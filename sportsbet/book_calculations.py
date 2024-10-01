
def decimal_to_american(decimal_odds):
    if decimal_odds >= 2.0:
        # Positive odds
        american_odds = (decimal_odds - 1) * 100
    else:
        # Negative odds
        american_odds = -100 / (decimal_odds - 1)
    return round(american_odds)


def american_to_decimal(american_odds):
    if american_odds > 0:
        return (american_odds / 100) + 1
    else:
        return (100 / abs(american_odds)) + 1


def imp_win(odds):
    if odds < 0:
        implied = abs(odds) / (abs(odds) + 100)
    else:
        implied = 100 / (odds + 100)
    return implied


def odds_format(odds):
    return f'+{odds}' if odds > 0 else str(odds)



def get_fair_prob(odds_1, odds_2):
    # Implied win %
    odds_1_imp = imp_win(odds_1)
    odds_2_imp = imp_win(odds_2)


    # Vig
    vig = odds_1_imp + odds_2_imp - 1

    # Fair win %
    team_1_fair = odds_1_imp / (1 + vig)
    team_2_fair = odds_2_imp / (1 + vig)


    return [team_1_fair, team_2_fair]


def get_fair_odds(odds_1, odds_2):
    fair_win_probs = get_fair_prob(odds_1, odds_2)

    # Fair win %
    team_1_fair = fair_win_probs[0]
    team_2_fair = fair_win_probs[1]

    # Fair odds
    odds_1_fair = decimal_to_american(1 / team_1_fair)
    odds_2_fair = decimal_to_american(1 / team_2_fair)


    return [odds_format(odds_1_fair), odds_format(odds_2_fair)]


def calculate_ev(book_odds, sharp_odds, sharp_odds_opp):
    sharp_imp = imp_win(sharp_odds)
    opp_imp = imp_win(sharp_odds_opp)
    sharp_vig = sharp_imp + opp_imp - 1
    fair_sharp = sharp_imp / (1 + sharp_vig)
    fair_opp = opp_imp / (1 + sharp_vig)

    if book_odds < 0:
        profit = abs(100 / book_odds)
    else:
        profit = book_odds / 100

    ev = round((fair_sharp * profit - fair_opp) * 100, 2)
    return ev


def kelly_criterion(probability, american_odds):
    # Convert American odds to decimal odds
    decimal_odds = american_to_decimal(american_odds)

    # Calculate the odds multiplier (b in the formula)
    b = decimal_odds - 1

    # Calculate the Kelly Criterion fraction
    kelly_fraction = ((b * probability) - (1 - probability)) / b

    # Ensure the fraction is between 0 and 1 (i.e., bet size cannot be negative or greater than the entire bankroll)
    kelly_fraction = max(0, min(kelly_fraction, 1))

    return kelly_fraction
