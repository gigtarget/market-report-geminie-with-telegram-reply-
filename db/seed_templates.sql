INSERT INTO narrative_templates
(name, direction, strength, leader, template_text, is_active, priority)
VALUES
('post_market_opening','up','strong','banknifty','Bank Nifty led the rally, closing up {banknifty_pct}.',true,2),
('post_market_opening','up','solid','nifty','Broad strength pushed Nifty higher by {nifty_pct}.',true,1),
('post_market_opening','mixed','any','any','A mixed close as sector rotation played out.',true,0);
